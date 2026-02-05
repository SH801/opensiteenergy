sudo chown -R www-data:www-data ../opensiteenergy
git reset --hard origin
git pull
sudo chown -R www-data:www-data ../opensiteenergy
sudo systemctl restart opensiteenergy.service
sudo systemctl restart tileserver.service
sudo apache2ctl restart