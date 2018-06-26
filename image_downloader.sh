#!/bin/bash
for pid in $(pidof -x image_downloader.sh); do
    if [ $pid != $$ ]; then
      kill -9 $pid
    fi
done

for pid in $(pidof -x image_downloader.py); do
      pkill -9 -P $pid
      kill -9 $pid
done

/home/django/ImageDownloader/image_downloader.py
