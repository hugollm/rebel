from setuptools import setup, find_packages

long_description = """
A library to ease the writing of vanilla SQL in Python.
It provides an easy to use interface to common types of queries.
"""

setup(
    name='rebel',
    version='0.0.0',
    description='Vanilla SQL for Python',
    long_description=long_description,
    url='https://github.com/hugollm/rebel',
    author='Hugo Mota',
    author_email='hugo.txt@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='rebel vanilla sql database',
    packages=find_packages(exclude=['tests', 'tests.driver_tests']),
)
