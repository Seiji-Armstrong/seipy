"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='seipy',
    version='1.1.0b1',
    description='Helper functions for data science',
    url='https://github.com/Seiji-Armstrong/seipy',
    author_email='seiji.armstrong@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers, Data Scientists, Machine Learning Engineers',
        'Topic :: Data Science and Machine Learning:: Helper functions',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='pandas numpy spark jupyter data-science machine-learning s3',
    packages=find_packages(exclude=[]),

    # This field lists other packages that your project depends on to run.
    # Any package you put here will be installed by pip when your project is
    # installed, so they must be valid existing projects.
    #
    # For an analysis of "install_requires" vs pip's requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['pandas', 'scipy', 'sklearn', 'numpy', 'pandas',
                      'matplotlib', 'scapy-python3', 'matplotlib', 'seaborn',
                      'IPython', 'boto3', 'pyspark'],
    python_requires='>=3'
)