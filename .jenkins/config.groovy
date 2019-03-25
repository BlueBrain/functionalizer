node {
    library identifier: 'bbp@master', retriever: modernSCM(
        [$class:'GitSCMSource',
         remote: 'ssh://bbpcode.epfl.ch/hpc/jenkins-pipeline',
         changelog: false])

    spack("spykfunc",
          "ssh://bbpcode.epfl.ch/building/Spykfunc",
          test: "")
}
