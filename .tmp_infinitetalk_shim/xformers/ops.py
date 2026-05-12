import torch
import torch.nn.functional as F


class _BlockDiagonalMask:
    @staticmethod
    def from_seqlens(*args, **kwargs):
        return None


class _AttnBiasModule:
    BlockDiagonalMask = _BlockDiagonalMask


class _FmhAModule:
    attn_bias = _AttnBiasModule()


fmha = _FmhAModule()


def memory_efficient_attention(q, k, v, attn_bias=None, op=None):
    # xformers expects [B, M, H, K]; torch SDPA expects [B, H, M, K].
    q_t = q.permute(0, 2, 1, 3).contiguous()
    k_t = k.permute(0, 2, 1, 3).contiguous()
    v_t = v.permute(0, 2, 1, 3).contiguous()
    out = F.scaled_dot_product_attention(q_t, k_t, v_t, attn_mask=None, dropout_p=0.0, is_causal=False)
    return out.permute(0, 2, 1, 3).contiguous()
