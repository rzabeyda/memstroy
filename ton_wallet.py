"""
Custodial TON wallet system for Memstroy bot.
"""
import os
import aiohttp

TONCENTER_API = "https://toncenter.com/api/v2"
TONCENTER_KEY = os.getenv("TONCENTER_API_KEY", "")

def generate_wallet():
    from tonsdk.contract.wallet import WalletVersionEnum, Wallets
    mnemonics, pub_k, priv_k, wallet = Wallets.create(WalletVersionEnum.v4r2, workchain=0)
    address = wallet.address.to_string(True, True, False)
    return mnemonics, address

def wallet_from_mnemonic(mnemonics: list):
    from tonsdk.contract.wallet import WalletVersionEnum, Wallets
    _, _, _, wallet = Wallets.from_mnemonics(mnemonics, WalletVersionEnum.v4r2, workchain=0)
    return wallet

async def get_seqno(address: str) -> int:
    """Get wallet seqno via runGetMethod"""
    headers = {}
    if TONCENTER_KEY:
        headers["X-API-Key"] = TONCENTER_KEY
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{TONCENTER_API}/runGetMethod",
                json={"address": address, "method": "seqno", "stack": []},
                headers=headers
            ) as r:
                data = await r.json()
                if data.get("ok"):
                    stack = data["result"].get("stack", [])
                    if stack:
                        return int(stack[0][1], 16)
    except Exception as e:
        print(f"get_seqno error: {e}")
    return 0

async def send_ton(mnemonics: list, to_address: str, amount_nano: int, comment: str = "") -> str:
    """Send TON from wallet to address."""
    from tonsdk.utils import bytes_to_b64str
    
    wallet = wallet_from_mnemonic(mnemonics)
    addr_str = wallet.address.to_string(True, True, False)
    seqno = await get_seqno(addr_str)
    print(f"Sending {amount_nano} nanoton to {to_address}, seqno={seqno}")

    query = wallet.create_transfer_message(
        to_addr=to_address,
        amount=amount_nano,
        seqno=seqno,
        payload=comment if comment else None,
    )
    boc = bytes_to_b64str(query["message"].to_boc(False))

    headers = {}
    if TONCENTER_KEY:
        headers["X-API-Key"] = TONCENTER_KEY

    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{TONCENTER_API}/sendBoc",
            json={"boc": boc},
            headers=headers
        ) as r:
            data = await r.json()
            if not data.get("ok"):
                raise Exception(data.get("error", "TON send failed"))
            return data.get("result", {}).get("hash", "ok")
