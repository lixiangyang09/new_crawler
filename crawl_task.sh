export PYTHONPATH=$HOME/repos:$HOME/repos/futile

cd /data/lixiangyang/new_crawler
echo "git pull"
git pull
echo "source"
. .venv/bin/activate
echo "start crawl"
python main.py
echo "gen report"
python report.py