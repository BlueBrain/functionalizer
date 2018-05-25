#include "spykfunc_udfs_hadoken_Random.h"
#include "Random.h"

jfieldID
getHandleField(JNIEnv *env, jobject obj)
{
	auto c = env->GetObjectClass(obj);
	return env->GetFieldID(c, "nativeHandle", "J");
}

rng*
getHandle(JNIEnv *env, jobject obj)
{
	jlong handle = env->GetLongField(obj, getHandleField(env, obj));
	return reinterpret_cast<rng*>(handle);
}

void
setHandle(JNIEnv *env, jobject obj, rng* r)
{
	jlong handle = reinterpret_cast<jlong>(r);
	env->SetLongField(obj, getHandleField(env, obj), handle);
}

jlong
Java_spykfunc_udfs_hadoken_Random_createThreefry(JNIEnv *env, jobject obj)
{
	hadoken::counter_engine<hadoken::threefry4x64> threefry_random_engine;
	return reinterpret_cast<jlong>(new rng(threefry_random_engine));
}


jlong
Java_spykfunc_udfs_hadoken_Random_internalCreate(JNIEnv *env, jclass obj, jlong seed, jlong key1, jlong key2)
{
	hadoken::counter_engine<hadoken::threefry4x64> threefry_random_engine;
	rng orig(threefry_random_engine);
	orig.seed(seed);
	rng* r = new rng(orig.derivate(key1).derivate(key2));
	return reinterpret_cast<jlong>(r);
}


jlong
Java_spykfunc_udfs_hadoken_Random_internalDerivate(JNIEnv *env, jobject obj, jlong key)
{
	rng* r = getHandle(env, obj);
	rng* s = new rng(r->derivate(static_cast<long>(key)));
	return reinterpret_cast<jlong>(s);
}

void
Java_spykfunc_udfs_hadoken_Random_setSeed(JNIEnv *env, jobject obj, jlong seed)
{
	rng* r = getHandle(env, obj);
	r->seed(seed);
}

void
Java_spykfunc_udfs_hadoken_Random_dispose(JNIEnv *env, jobject obj)
{
	rng* r = getHandle(env, obj);
	if (r != 0)
		delete r;
	setHandle(env, obj, 0);
}

jlong
Java_spykfunc_udfs_hadoken_Random_nextLong(JNIEnv *env, jobject obj)
{
	rng* r = getHandle(env, obj);
	return jlong((*r)());
}

jdouble
Java_spykfunc_udfs_hadoken_Random_nextDouble(JNIEnv *env, jobject obj)
{
	rng* r = getHandle(env, obj);
	return ((*r)()) * (1.0 / MAXVAL);
}
