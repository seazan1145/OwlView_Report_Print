# OwlView Report Print

## Selenium / ChromeDriver 運用

このツールは **Selenium Manager による自動ドライバ解決** を利用します。

- `chromedriver.exe` の固定同梱・固定パス指定は行いません。
- WebDriver 起動は `webdriver.Chrome(options=options)` で実行します。
- 初回起動時や Chrome 更新後は、Selenium Manager が必要なドライバ取得を行うため、起動に時間がかかる場合があります。

## ネットワーク制限がある環境での注意

社内ネットワークやプロキシ制限で自動取得に失敗すると、Chrome 起動時にエラーになります。
その場合はログに以下の観点を表示します。

- Chrome のインストール状態
- Selenium パッケージ状態
- ネットワーク / プロキシ制限
- Selenium Manager の自動取得失敗

## 既存環境からの移行

- 既存の `chromedriver.exe` が残っていても、本アプリは固定参照しません。
- `PATH` 上の ChromeDriver を前提にした運用は不要です。
