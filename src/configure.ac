#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.61])
AC_INIT([OverpassAPI], [0.7.54.13], [roland@olbricht.nrw])
AC_CONFIG_SRCDIR([template_db/file_blocks.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([-Wall foreign subdir-objects -Wno-portability])

# Checks for programs.
AM_PROG_AR
AC_PROG_CC
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_LIBTOOL
AC_PROG_MAKE_SET

AX_CXX_COMPILE_STDCXX_11( ,[mandatory])

# Checks for libraries.
AC_CHECK_LIB([expat], [XML_Parse])
AC_SEARCH_LIBS([shm_open], [rt])

# Checks for header files.
AC_TYPE_MODE_T
AC_TYPE_PID_T
AC_TYPE_SIGNAL
AC_TYPE_SIZE_T
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h locale.h stdint.h stdlib.h string.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_C_VOLATILE
AC_HEADER_STDBOOL
AC_HEADER_TIME
AC_STRUCT_TM

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([clock_gettime floor ftruncate getcwd munmap regcomp select shm_open socket sqrt strnlen])


AC_ARG_ENABLE([lz4],
     AS_HELP_STRING([--enable-lz4],[enable lz4 compression algorithm]),
     [case "${enableval}" in
       yes) enable_lz4=true ;;
       no)  enable_lz4=false ;;
       *) AC_MSG_ERROR([bad value ${enableval} for --enable-lz4]) ;;
     esac],[enable_lz4=false])
AS_IF([test x$enable_lz4 != xfalse], [want_lz4="yes"], [want_lz4="no"])

COMPRESS_LIBS="-lz"
AC_SUBST(COMPRESS_LIBS, ["$COMPRESS_LIBS"])

if test "$want_lz4" != "no"; then
  AC_CHECK_HEADER(lz4.h, [
    AC_CHECK_LIB(lz4, LZ4_compress, [
      AC_DEFINE(HAVE_LZ4, 1, [Define if you have lz4 library])
      COMPRESS_LIBS="$COMPRESS_LIBS -llz4"
    ], [
      if test "$want_lz4" = "yes"; then
	    AC_ERROR([Can't build with lz4 support: liblz4 not found])
      fi
    ])
  ], [
    if test "$want_lz4" = "yes"; then
      AC_ERROR([Can't build with lz4 support: lz4.h not found])
    fi
  ])
fi

AC_SUBST(COMPRESS_LIBS, ["$COMPRESS_LIBS"])

AC_ARG_ENABLE([tests],
     AS_HELP_STRING([--enable-tests],[enable Overpass API test suite]),
     [case "${enableval}" in
       yes) tests=true ;;
       no)  tests=false ;;
       *) AC_MSG_ERROR([bad value ${enableval} for --enable-tests]) ;;
     esac],[tests=false])

AM_CONDITIONAL([COND_TESTS], [test x$tests = xtrue])

# FastCGI

AC_LANG_PUSH([C++])

AC_ARG_ENABLE([fastcgi],
     AS_HELP_STRING([--enable-fastcgi],[enable FastCGI]),
     [case "${enableval}" in
       yes) enable_fastcgi=true ;;
       no)  enable_fastcgi=false ;;
       *) AC_MSG_ERROR([bad value ${enableval} for --enable-fastcgi]) ;;
     esac],[enable_fastcgi=false])
AS_IF([test x$enable_fastcgi != xfalse], [want_fastcgi="yes"], [want_fastcgi="no"])

FASTCGI_LIBS=""

if test "$want_fastcgi" != "no"; then
  AC_CHECK_HEADER([fcgio.h], [
    AC_CHECK_LIB(fcgi, FCGX_Accept_r, [
      AC_DEFINE(HAVE_FASTCGI, 1, [Define if you have FastCGI library])
      FASTCGI_LIBS="$FASTCGI_LIBS -lfcgi++ -lfcgi"
    ], [
      if test "$want_fastcgi" = "yes"; then
	    AC_ERROR([Can't build with fastcgi support: libfcgi not found])
      fi
    ])
  ], [
    if test "$want_fastcgi" = "yes"; then
      AC_ERROR([Can't build with FastCGI support: unable to find FastCGI header files])
    fi
  ])
fi

AC_SUBST(FASTCGI_LIBS, ["$FASTCGI_LIBS"])     
AC_LANG_POP([C++])



AC_DEFINE(HAVE_LIBOSMIUM, 1, [Define if you have libosmium library])
AC_DEFINE(HAVE_ICU, 1, [Define if you have ICU library])
AC_DEFINE(HAVE_PCRE, 1, [Define if you have PCRE library])
AC_DEFINE(HAVE_OPENMP, 1, [Define if you have OpenMP])

# Regular expression libs ICU
ICU_LIBS=""
ICU_LIBS="-licuuc -licui18n"
AC_SUBST(ICU_LIBS, ["$ICU_LIBS"])

PCRE_LIBS=""
PCRE_LIBS="-lpcre2-8"
AC_SUBST(PCRE_LIBS, ["$PCRE_LIBS"])


OPENMP_FLAG=""
OPENMP_FLAG="-fopenmp"
AC_SUBST(OPENMP_FLAG, ["$OPENMP_FLAG"])

PTHREAD_LIBS="-lpthread"
AC_SUBST(PTHREAD_LIBS, ["$PTHREAD_LIBS"])

BZ2_LIBS="-lbz2"
AC_SUBST(BZ2_LIBS, ["$BZ2_LIBS"])


AC_CONFIG_FILES([Makefile])

if test "$tests" = "true"; then
  AC_CONFIG_FILES([test-bin/Makefile])
fi

AC_OUTPUT
AC_SYS_LARGEFILE
