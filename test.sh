if [ ! -d instance ]; then
    echo "setup dashboard"
    git clone https://github.com/userdashboard/dashboard.git instance
    cd instance
    npm install mysql mocha puppeteer@2.1.1 --no-save
else 
    echo "existing dashboard"
    cd instance
fi
echo "copy storage module"
rm -rf node_modules/@userdashboard/storage-mysql
mkdir -p node_modules/@userdashboard/storage-mysql
cp ../index.js node_modules/@userdashboard/storage-mysql
cp ../setup.sql node_modules/@userdashboard/storage-mysql
cp -R ../src node_modules/@userdashboard/storage-mysql
echo "start testing"
echo `ls`
NODE_ENV=testing \
FAST_START=true \
DASHBOARD_SERVER=http://localhost:9000 \
DOMAIN=localhost \
STORAGE_ENGINE=@userdashboard/storage-mysql \
DATABASE_URL=mysql://root:password@127.0.0.1:8888/test  \
GENERATE_SITEMAP_TXT=false \
GENERATE_API_TXT=false \
npm test