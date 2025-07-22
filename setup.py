from setuptools import setup, find_packages

# Function to read the requirements.txt file
def load_requirements(filename='requirements.txt'):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

def readme():
    with open('README.md', encoding='utf-8') as f:
        return f.read()

setup(
    name='aiq-datasets',
    version='0.1.0',
    packages=find_packages(),
    install_requires=load_requirements(),
    author="darrenwang",
    author_email="wangyang9113@gmail.com",
    description="aiq datasets",
    long_description=readme(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6"
)