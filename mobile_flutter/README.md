# Fund Quant Mobile (Flutter)

This folder is the Flutter mobile client for the existing Python backend in this repository.

## 1) Prerequisites

- Install Flutter SDK (stable channel)
- Install Android Studio and/or Xcode
- Confirm tooling:

```bash
flutter doctor
```

## 2) Generate platform runners (android/ios)

Run this once in the `mobile_flutter` directory to create Android/iOS native folders:

```bash
cd mobile_flutter
flutter create --platforms=android,ios .
```

## 3) Install dependencies

```bash
flutter pub get
```

## 4) Configure backend API URL

Use `--dart-define` for runtime environment selection:

```bash
flutter run --dart-define=API_BASE_URL=http://192.168.1.100:8000
```

Notes:
- Android emulator usually uses `http://10.0.2.2:8000`.
- Physical phone should use your computer LAN IP.
- iOS devices should use HTTPS or local network permission as needed.
- Public deployment can use `http://<public-ip>:8000` for quick validation, then switch to HTTPS for production.

## 5) Build installable packages

Android APK:

```bash
flutter build apk --release --dart-define=API_BASE_URL=https://your-api-domain
```

iOS IPA (requires macOS + Xcode + signing):

```bash
flutter build ipa --release --dart-define=API_BASE_URL=https://your-api-domain
```

## API endpoints used

- `GET /api/recommendations`
- `GET /api/sector_fund_flow`
