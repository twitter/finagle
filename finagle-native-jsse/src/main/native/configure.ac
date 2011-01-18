# -*- Autoconf -*-

AC_PREREQ([2.65])
AC_INIT([finagle-native], [1.0], [wilhelm@twitter.com])
AC_CONFIG_SRCDIR([addr.h])
AC_CONFIG_HEADERS([config.h])

AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE
LT_INIT

# Checks for programs.
AC_PROG_CC
AX_PROG_JAVAC([], [AC_ERROR([javac was not found])])

# Checks for libraries.
AX_CHECK_OPENSSL([], [AC_ERROR([OpenSSL not found])])
CFLAGS="$CFLAGS $OPENSSL_INCLUDES"
LDFLAGS="$LDFLAGS $OPENSSL_LIBS"

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdint.h unistd.h openssl/bio.h],
                 [], [AC_ERROR([required headers are missing])])

AC_JNI_INCLUDE_DIR

for JNI_INCLUDE_DIR in $JNI_INCLUDE_DIRS
do
        CFLAGS="$CFLAGS -I$JNI_INCLUDE_DIR"
done

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset strstr])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
