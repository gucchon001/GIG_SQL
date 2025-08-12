"""
塾ステ CSVダウンロードツール「ストミンくん」セットアップ
"""
from setuptools import setup, find_packages

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="jukuste-csv-tool",
    version="1.0.0",
    description="塾ステ CSVダウンロードツール「ストミンくん」",
    author="Development Team",
    author_email="dev@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=requirements,
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "jukuste-batch=batch_system.main:main_production",
            "jukuste-batch-test=batch_system.main:main_test",
            "jukuste-batch-rawdata=batch_system.main:main_rawdata",
            "jukuste-streamlit=streamlit_system.app:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)