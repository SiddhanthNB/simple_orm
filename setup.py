from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="simple-orm",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A lightweight, ActiveRecord-inspired ORM layer built on top of SQLAlchemy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/simple-orm",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "SQLAlchemy>=2.0.0",
        "pydantic>=2.0.0",
        "python-dotenv>=1.0.0",
        "PyYAML>=6.0.0",
    ],
    extras_require={
        "postgresql": ["psycopg[binary]>=3.1.0"],
        "mysql": ["PyMySQL>=1.0.0"],
        "sqlite": [],  # Built into Python
        "oracle": ["cx_Oracle>=8.0.0"],
        "mssql": ["pyodbc>=4.0.0"],
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "pre-commit>=3.0.0",
        ],
        "docs": [
            "sphinx>=6.0.0",
            "sphinx-rtd-theme>=1.2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            # Add CLI commands here if needed
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/yourusername/simple-orm/issues",
        "Source": "https://github.com/yourusername/simple-orm",
        "Documentation": "https://simple-orm.readthedocs.io/",
    },
)