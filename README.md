Hit Data Analysis
---

Problem - Using Hit Data analyze the revenue generated from external search engines like Google, Bing and Yahoo etc.
 



INSTALLATION -
----
1. Install Python3.9
2. Install pip
3. Prepare virtual environment to install expected python modules
4. Install requirements.txt
5. Setup GitHub repo - https://github.com/SonawaneMayur/HitAnalysis.git

EXECUTION -
----
1. Update analysis/config.py file
2. Use following command to run application -
   python stats.py {input_file_path}
3. To verify output checkout the output path, which will have {YYYY-MM-DD}_SearchKeywordPerformance.tab file with data
4. Before pushing code to GitHub, unit test it
5. Push code to feature branch, which will get merge into master branch
6. Use AWS cloud template to run app on AWS EMR
7. Use Databricks template to run app on Databricks with AWS backend
8. Log file inside logs contains application logs to debug run


Tags: 
-----
Python, Pyspark, GitHub, AWS, Databricks

AUTHOR:
------
  Mayur Soanwane
