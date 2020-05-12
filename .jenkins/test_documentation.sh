cd ..

module load unstable boost cmake python

if [ -z "${GERRIT_CHANGE_NUMBER}" ]; then
    python setup.py build_docs --upload
else
    python setup.py build_docs
fi
