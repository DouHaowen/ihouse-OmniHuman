/* @jclaw/lab-sdk — 免构建浏览器版(IIFE)。生产请用 npm 包 import。 */
(function(){

// ---- protocol ----
// 实验室小程序 ↔ 宿主 的通信协议(端无关,postMessage 之上)。
// 三端(移动 react-native-webview / Electron <webview> / Web iframe)共用同一份。
function isBridgeResponse(value) {
    return (typeof value === 'object' &&
        value !== null &&
        typeof value.id === 'string' &&
        typeof value.ok === 'boolean');
}
function isBridgeEvent(value) {
    return (typeof value === 'object' &&
        value !== null &&
        typeof value.event === 'string');
}


// ---- capabilities ----
// 能力清单 —— 单一事实源(宿主、SDK、注册中心 scope 三处同源)。
// 每个 ns.method 声明它需要的 scope(无 scope = 无需授权,如 ui.toast)。
const CAPABILITIES = {
    auth: {
        getUser: { scope: 'auth.read' },
        getToken: { scope: 'auth.token' },
    },
    navigation: {
        setTitle: {},
        setRightButton: {},
        pop: {},
        openApp: { scope: 'nav.open' },
    },
    ui: {
        toast: {},
        confirm: {},
        loading: {},
        setNavBar: {},
    },
    request: {
        request: { scope: 'api.read' },
    },
    file: {
        upload: { scope: 'file.upload' },
    },
    device: {
        scanCode: { scope: 'device.camera' },
        pickImage: { scope: 'device.camera' },
    },
    storage: {
        get: {},
        set: {},
    },
    lifecycle: {
        ready: {},
    },
};
/** 查某 ns.method 的 spec;不存在返回 undefined(宿主据此回 UNKNOWN_METHOD)。 */
function capabilitySpec(ns, method) {
    const group = CAPABILITIES[ns];
    if (!group)
        return undefined;
    return group[method];
}


// ---- inject ----
// 宿主侧注入助手。SDK 走标准 postMessage,但 react-native-webview 的「宿主→小程序」
// 方向需要宿主用 injectJavaScript 主动调一个全局函数。这里给出该全局名与生成器。
//
// 约定:
//  - 小程序→宿主:RN 用 window.ReactNativeWebView.postMessage;iframe 用 parent.postMessage。
//  - 宿主→小程序:RN 用 injectJavaScript 调 window.__JCLAW_RECEIVE__(json);iframe 用 postMessage。
// SDK(sdk.ts)两条通道都监听,作者无感。
/** 小程序侧接收宿主消息的全局函数名(RN 通道用)。 */
const RECEIVE_GLOBAL = '__JCLAW_RECEIVE__';
/** 标记运行环境与 appKey 的引导脚本,injectedJavaScriptBeforeContentLoaded 用。 */
function bootstrapScript(env) {
    return `window.__JCLAW_ENV__ = ${JSON.stringify(env)};true;`;
}
/** RN 宿主把一条应答/事件送回小程序时,injectJavaScript 执行的脚本。 */
function deliverScript(message) {
    // 末尾 true; 避免 injectJavaScript 返回非法值告警。
    return `window.${RECEIVE_GLOBAL} && window.${RECEIVE_GLOBAL}(${JSON.stringify(message)});true;`;
}


// ---- dispatcher ----
// 宿主侧通用分发器(端无关)。校验 scope、路由到 handler、回传结果。
// 各端只需提供:handler 注册表、hasScope 判定、post(把应答送回小程序)。
// `toast`/`scanCode` 这类碰原生的 handler 由各端实现并注入此处。
function fail(id, code, message) {
    return { id, ok: false, error: { code, message } };
}
/**
 * 返回一个 handleRequest(raw)。各端在收到小程序消息时调用它。
 * raw 可以是已解析的 BridgeRequest,或原始 JSON 字符串。
 */
function createDispatcher(options) {
    const { appKey, handlers, hasScope, post } = options;
    return async function handleRequest(raw) {
        let req;
        try {
            req = typeof raw === 'string' ? JSON.parse(raw) : raw;
        }
        catch {
            return; // 不可解析的消息直接忽略(可能不是给桥的)
        }
        if (!req || typeof req.id !== 'string' || typeof req.ns !== 'string' || typeof req.method !== 'string') {
            return;
        }
        const spec = capabilitySpec(req.ns, req.method);
        if (!spec) {
            post(fail(req.id, 'UNKNOWN_METHOD', `${req.ns}.${req.method} not found`));
            return;
        }
        if (spec.scope && !hasScope(spec.scope)) {
            post(fail(req.id, 'PERMISSION_DENIED', `missing scope ${spec.scope}`));
            return;
        }
        const handler = handlers[req.ns]?.[req.method];
        if (!handler) {
            post(fail(req.id, 'UNKNOWN_METHOD', `${req.ns}.${req.method} has no host handler`));
            return;
        }
        try {
            const result = await handler(req.args, { appKey, ns: req.ns, method: req.method });
            post({ id: req.id, ok: true, result });
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            post(fail(req.id, 'INTERNAL', message));
        }
    };
}


// ---- sdk ----
// 小程序侧 SDK —— 团队 `import { jclaw } from '@jclaw/lab-sdk'` 即用。
// 自动识别运行环境(RN webview / iframe / 纯浏览器 mock),对作者透明。
function detectTransport() {
    if (typeof window === 'undefined')
        return 'mock';
    if (window.ReactNativeWebView)
        return 'rn';
    if (window.parent && window.parent !== window)
        return 'iframe';
    return 'mock';
}
class LabClient {
    constructor() {
        this.seq = 0;
        this.pending = new Map();
        this.listeners = new Map();
        this.mocks = {};
        this.auth = {
            getUser: () => this.call('auth', 'getUser'),
            getToken: () => this.call('auth', 'getToken'),
        };
        this.navigation = {
            setTitle: (title) => this.call('navigation', 'setTitle', { title }),
            setRightButton: (text) => this.call('navigation', 'setRightButton', { text }),
            pop: () => this.call('navigation', 'pop'),
            openApp: (appKey) => this.call('navigation', 'openApp', { appKey }),
        };
        this.ui = {
            toast: (text) => this.call('ui', 'toast', { text }),
            confirm: (text) => this.call('ui', 'confirm', { text }),
            loading: (show) => this.call('ui', 'loading', { show }),
            setNavBar: (config) => this.call('ui', 'setNavBar', config),
        };
        this.request = (input) => this.call('request', 'request', input);
        this.file = {
            upload: (blob) => this.call('file', 'upload', { blob }),
        };
        this.device = {
            scanCode: () => this.call('device', 'scanCode'),
            pickImage: () => this.call('device', 'pickImage'),
        };
        this.storage = {
            get: (key) => this.call('storage', 'get', { key }),
            set: (key, value) => this.call('storage', 'set', { key, value }),
        };
        this.transport = detectTransport();
        this.env = this.transport;
        if (typeof window !== 'undefined') {
            // RN 通道:宿主 injectJavaScript 调此全局。
            window[RECEIVE_GLOBAL] = (msg) => this.receive(msg);
            // iframe 通道:监听 postMessage。
            window.addEventListener('message', (ev) => this.receive(ev.data));
        }
    }
    receive(raw) {
        let msg = raw;
        if (typeof raw === 'string') {
            try {
                msg = JSON.parse(raw);
            }
            catch {
                return;
            }
        }
        if (isBridgeResponse(msg)) {
            const p = this.pending.get(msg.id);
            if (!p)
                return;
            this.pending.delete(msg.id);
            if (msg.ok)
                p.resolve(msg.result);
            else
                p.reject(new Error(msg.error ? `${msg.error.code}: ${msg.error.message}` : 'bridge error'));
            return;
        }
        if (isBridgeEvent(msg)) {
            const set = this.listeners.get(msg.event);
            if (set)
                set.forEach((cb) => cb(msg.payload));
        }
    }
    send(req) {
        if (this.transport === 'rn') {
            window.ReactNativeWebView?.postMessage(JSON.stringify(req));
        }
        else if (this.transport === 'iframe') {
            window.parent.postMessage(req, '*');
        }
    }
    call(ns, method, args) {
        if (this.transport === 'mock') {
            const handler = this.mocks[ns]?.[method];
            if (!handler) {
                return Promise.reject(new Error(`mock 未注册 ${ns}.${method}`));
            }
            return Promise.resolve(handler(args));
        }
        const id = `c-${++this.seq}`;
        const promise = new Promise((resolve, reject) => {
            this.pending.set(id, { resolve: resolve, reject });
        });
        this.send({ id, ns, method, args });
        return promise;
    }
    /** 是否运行在真实宿主(RN/iframe)里;纯浏览器独立开发时为 false。 */
    isHosted() {
        return this.env !== 'mock';
    }
    /** 纯浏览器开发时注册桩,绕开宿主联调。生产代码可用 `if (!jclaw.isHosted())` 守卫。 */
    mock(handlers) {
        this.mocks = handlers;
        this.transport = 'mock';
    }
    /** 订阅宿主事件(返回键、网络变化等),返回取消订阅函数。 */
    on(event, cb) {
        let set = this.listeners.get(event);
        if (!set) {
            set = new Set();
            this.listeners.set(event, set);
        }
        set.add(cb);
        return () => set?.delete(cb);
    }
    ready() {
        // 非宿主(本地 mock)无需等待,直接就绪 —— 团队不必为 ready 注册 mock。
        if (this.transport === 'mock')
            return Promise.resolve();
        return this.call('lifecycle', 'ready');
    }
}
/** 默认单例,小程序直接用。 */
const jclaw = new LabClient();


  window.jclaw = jclaw;
  window.LabSDK = { jclaw, createDispatcher, capabilitySpec, CAPABILITIES, bootstrapScript, deliverScript, RECEIVE_GLOBAL };
})();
