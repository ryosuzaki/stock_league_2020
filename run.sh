sudo yum update
sudo yum install python3 -y
python3 -m venv myenv
source myenv/bin/activate
pip install pandas
pip install pytrends
cd /home/ec2-user
python getInterestOverTime.py