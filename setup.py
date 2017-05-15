from setuptools import setup, find_packages

setup(
    name='sw_pylib',
    version='1.0.0',
    packages=find_packages(),
    zip_safe=True,
    install_requires=['invoke==0.12.2', 'tld', 'redis', 'happybase', 'snakebite', 'thriftpy==0.3.8', 'ujson==1.33',
                      'mrjob==0.5.2', 'consulate==0.6.0', 'python-etcd==0.3.3', 'urllib3==1.16'],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest',
        'python-dateutil',
        'boto>=2.42.0',
        'snakebite',
        'invoke==0.12.2',
        'redis',
        'ujson==1.33',
        'mrjob==0.5.2',
        'jsonschema==2.3.0',
        'mock==2.0.0',
        'testscenarios==0.5.0',
        'testresources==2.0.1',
        'virtualenv==15.1.0',
        'contextlib2==0.5.4'],
    test_suite='tests',
    entry_points={
        'console_scripts': ['ptask = pylib.tasks.ptask_invoke:main']
    },
    include_package_data=True
)
