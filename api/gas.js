// Vercel 중계 함수: 사내 PC가 구글(script.google.com)에 직접 접속하지 못해도
// 화면 → Vercel(/api/gas) → 구글 Apps Script 순서로 대신 전달합니다.
// 웹 앱 주소를 바꾸면 아래 GAS_URL 한 줄만 고치면 됩니다.
const GAS_URL = "https://script.google.com/macros/s/AKfycbzBdxMCMeVeIM9coKr_xAHTn7vD0lttAWU53L_9BTHFldvEvdHoQttIiEx_MKCYWEFN/exec";

module.exports = async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  try {
    const q = req.url.indexOf("?");
    const qs = q >= 0 ? req.url.slice(q) : "";
    let r;
    if (req.method === "POST") {
      const body = typeof req.body === "string" ? req.body : JSON.stringify(req.body || {});
      r = await fetch(GAS_URL, {
        method: "POST",
        headers: { "Content-Type": "text/plain;charset=utf-8" },
        body,
        redirect: "follow"
      });
    } else {
      r = await fetch(GAS_URL + qs, { redirect: "follow" });
    }
    const text = await r.text();
    res.status(200).send(text);
  } catch (e) {
    res.status(200).send(JSON.stringify({ ok: false, error: "서버 연결에 실패했습니다. 잠시 후 다시 시도해 주세요." }));
  }
};
