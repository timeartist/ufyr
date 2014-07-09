from setuptools import setup, find_packages

setup(
    name='ufyr',
    version='1.0.0',
    description='Common Redis Related Code',
    author='Adi Foulger',
    author_email='adi@zefr.com',
    packages=find_packages(),
    zip_safe=False,
    install_requires= ('redis', 'rq'))
