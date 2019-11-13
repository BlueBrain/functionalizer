cd ..

module load unstable boost cmake

if [ -z "${GERRIT_CHANGE_NUMBER}" ]; then
    python setup.py build_docs --upload
else
    python setup.py build_docs
fi
