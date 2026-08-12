# 台股波段雷達 Live v1

這版已經不是只有 GitHub Pages 靜態畫面，而是「前端 + FastAPI 後端」完整專案。

## 功能
- 09:30 今日 Top 20：回踩買點、突破買點、不追價、停損、目標1/2
- 盤後 Top 20：同樣完整交易區間
- Fugle 即時報價接口
- Fugle Snapshot Actives 市場量能掃描
- 歷史 K 線計算 5 日均量、20 日高點與動態風控
- 原則排除千金股；高分例外
- 我的持股：成本、股數、即時/收盤價、報酬率、損益
- 第一/第二加碼、攤平區、禁止攤平、停損、目標
- PIN 修改保護，PIN 與持股存在瀏覽器 localStorage
- API Key 只存在伺服器環境變數

## Demo 啟動
pip install -r requirements.txt
uvicorn app:app --reload

## Live 啟動
環境變數：
FUGLE_API_KEY=你的金鑰
LIVE_MODE=true

## 部署
專案附 render.yaml，可部署到 Render。
Render 建立服務後，在 Environment 裡新增 FUGLE_API_KEY，不要提交到 GitHub。

## 注意
Fugle Snapshot Actives 屬特定付費方案功能；如果帳號方案不支援，雷達 endpoint 會無法完成全市場 Actives 掃描。
