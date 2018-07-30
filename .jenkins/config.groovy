node {
    library identifier: 'bbp@master', retriever: modernSCM(
        [$class:'GitSCMSource',
         remote: 'ssh://bbpcode.epfl.ch/hpc/jenkins-pipeline'])

    nix("mod-spykfunc-py3",
        "ssh://bbpcode.epfl.ch/building/Spykfunc")
}
