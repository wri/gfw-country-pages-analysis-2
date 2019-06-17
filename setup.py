from setuptools import setup, find_packages

setup(
    name="gfw_country_pages_analysis_2",
    version="1.0.0",
    description="Tool to create summary tables for GFW country pages",
    author="Charlie Hoffman",
    license="MIT",
    packages=find_packages(),
    data_files=[("config",
                 ["gfw_country_pages_analysis_2/config/fires_country_pages_prod.ini",
                  "gfw_country_pages_analysis_2/config/fires_country_pages_staging.ini",
                  "gfw_country_pages_analysis_2/config/fires_report_prod.ini",
                  "gfw_country_pages_analysis_2/config/fires_report_staging.ini",
                  "gfw_country_pages_analysis_2/config/glad_prod.ini",
                  "gfw_country_pages_analysis_2/config/glad_staging.ini",
                  "gfw_country_pages_analysis_2/config/terrai_prod.ini",
                  "gfw_country_pages_analysis_2/config/terrai_staging.ini"])],
    install_requires=[
        "gspread<=3.2.0,>=3.1.0",
        "oauth2client<=4.2.0,>=4.1.3",
        "retrying<=1.4.0,>=1.3.3",
        "pandas<=0.25.0,>=0.24.2",
        "awscli<=1.17.0,>=1.16.176",
        "boto3<=1.10.0,>=1.9.166",
        "rsa<=3.5.0,>=3.1.2",
        "hadoop_pip @ git+https://github.com/wri/hadoop_pip@v1.0.0#egg=hadoop_pip",
    ],
    entry_points={
        "console_scripts": [
            "update_country_stats=gfw_country_pages_analysis_2.update_country_stats:main",
        ]
    }
)
