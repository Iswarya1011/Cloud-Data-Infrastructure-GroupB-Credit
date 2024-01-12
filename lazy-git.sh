#!/bin/bash
set -e
black .

echo Type message
read -p "Message: " message

git commit -am "$message"
git push