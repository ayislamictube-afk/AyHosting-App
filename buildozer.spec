[app]
title = Ay Hosting Pro
package.name = ayhosting
package.domain = com.ay

source.dir = .
source.include_exts = py,png,jpg,kv,atlas,db,html,js,css

version = 4.0
requirements = python3,kivy,flask,requests,urllib3,setuptools

orientation = portrait
fullscreen = 1

android.permissions = INTERNET,ACCESS_NETWORK_STATE,ACCESS_WIFI_STATE,FOREGROUND_SERVICE,WAKE_LOCK
android.api = 33
android.minapi = 24
android.accept_sdk_license = True
android.wakelock = True

[buildozer]
log_level = 2
warn_on_root = 1
