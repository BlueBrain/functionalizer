/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class spykfunc_udfs_hadoken_Random */

#ifndef _Included_spykfunc_udfs_hadoken_Random
#define _Included_spykfunc_udfs_hadoken_Random
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     spykfunc_udfs_hadoken_Random
 * Method:    dispose
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_spykfunc_udfs_hadoken_Random_dispose
  (JNIEnv *, jobject);

/*
 * Class:     spykfunc_udfs_hadoken_Random
 * Method:    nextDouble
 * Signature: ()D
 */
JNIEXPORT jdouble JNICALL Java_spykfunc_udfs_hadoken_Random_nextDouble
  (JNIEnv *, jobject);

/*
 * Class:     spykfunc_udfs_hadoken_Random
 * Method:    nextLong
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_spykfunc_udfs_hadoken_Random_nextLong
  (JNIEnv *, jobject);

/*
 * Class:     spykfunc_udfs_hadoken_Random
 * Method:    setSeed
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_spykfunc_udfs_hadoken_Random_setSeed
  (JNIEnv *, jobject, jlong);

/*
 * Class:     spykfunc_udfs_hadoken_Random
 * Method:    createThreefry
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_spykfunc_udfs_hadoken_Random_createThreefry
  (JNIEnv *, jobject);

/*
 * Class:     spykfunc_udfs_hadoken_Random
 * Method:    internalCreate
 * Signature: (JJJ)J
 */
JNIEXPORT jlong JNICALL Java_spykfunc_udfs_hadoken_Random_internalCreate
  (JNIEnv *, jclass, jlong, jlong, jlong);

/*
 * Class:     spykfunc_udfs_hadoken_Random
 * Method:    internalDerivate
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_spykfunc_udfs_hadoken_Random_internalDerivate
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif
