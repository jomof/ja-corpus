import setuptools

setuptools.setup(
    name='ja-corpus-pipeline',
    version='1.0',
    install_requires=[],
    packages=setuptools.find_packages(where='scripts'),
    package_dir={'': 'scripts'},
    py_modules=['ja_sentence_extractor', 'corpus_pipeline'],
)
