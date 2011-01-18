# Configure native libs

NATIVE_LIB_ROOT="../hyjsse-twttr"
NATIVE_LIB_NAME="libfinaglenative.so"
NATIVE_LIB_DIR=`find $NATIVE_LIB_ROOT -name "$NATIVE_LIB_NAME" | head -1 | xargs dirname`

if [ -z "$NATIVE_LIB_DIR" ]; then
    echo "Native library not found." 1>&2
else
    echo "Native library found in $NATIVE_LIB_DIR"
    export JAVA_LIB_PATH="$NATIVE_LIB_DIR"
    export FINAGLE_NATIVE="1"
fi
