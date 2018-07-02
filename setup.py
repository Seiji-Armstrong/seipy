"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='seipy',
    version='1.3.2',
    description='Helper functions for data science',
    long_description=long_description,
    url='https://github.com/Seiji-Armstrong/seipy',
    author_email='seiji.armstrong@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='pandas numpy spark jupyter data-science machine-learning s3',
    packages=find_packages(exclude=[]),
    install_requires=['pandas',
                      'requests',
                      'scipy',
                      'scikit-learn',
                      'numpy',
                      'pandas',
                      'matplotlib',
                      'scapy-python3',
                      'seaborn',
                      'ipython',
                      'boto3',
                      'pyspark'],
    python_requires='>=3',
)
