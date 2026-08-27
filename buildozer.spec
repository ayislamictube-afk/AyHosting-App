[app]
title = Ay Hosting Pro
package.name = ayhosting
package.domain = com.ay

source.dir = .
source.include_exts = py,png,jpg,kv,atlas,db,html,js,css

version = 4.0
requirements = python3,hostpython3,kivy,flask,yt-dlp,requests,urllib3

orientation = portrait
fullscreen = 1

android.permissions = INTERNET,ACCESS_NETWORK_STATE,ACCESS_WIFI_STATE,FOREGROUND_SERVICE,WAKE_LOCK,WRITE_EXTERNAL_STORAGE,READ_EXTERNAL_STORAGE
android.api = 34
android.minapi = 24
android.ndk = 25b
android.archs = arm64-v8a, armeabi-v7a
android.wakelock = True

[buildozer]
log_level = 2
warn_on_root = 1
