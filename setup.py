from setuptools import setup, find_packages

setup(
    name="ES-Helper",
    version="0.1.0",
    author='Sadam·Sadik',
    author_email='Haoke98@outlook.com',
    description='一个用于ElasticSearch运维的命令行工具集',
    long_description=open('README.md', 'r', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Haoke98/ElasticSearchHelper',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
        "elasticsearch>=7.0.0",
        "ollama",
        "python-dotenv",
    ],
    entry_points={
        "console_scripts": [
            "es-helper=main:main",
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires='>=3.6',
)
