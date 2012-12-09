#############################################################################
#
#	SIB Daemon
#
#	Copyright (c) 2009 Nokia Corporation
#
#############################################################################

AC_INIT()
AC_CONFIG_AUX_DIR(config)

AM_INIT_AUTOMAKE(sibd, 0.4)
AM_CONFIG_HEADER(config.h)

#############################################################################
# Constants
#############################################################################

#############################################################################
# Localisation
#############################################################################
#GETTEXT_PACKAGE=$PACKAGE
#AC_SUBST(GETTEXT_PACKAGE)
#AC_DEFINE_UNQUOTED(GETTEXT_PACKAGE, 
#		   "$GETTEXT_PACKAGE", [Define gettext package])
#ALL_LINGUAS="en_GB"
#AM_GLIB_GNU_GETTEXT

#localedir=`$PKG_CONFIG osso-af-settings --variable=localedir`
#AC_SUBST(localedir)
#AC_DEFINE_UNQUOTED([PACKAGE_LOCALE_DIR], 
#		   ["${prefix}/${DATADIRNAME}/locale"], [Locale directory])

#############################################################################
# Checks for some common programs needed for configure/compile/install
#############################################################################

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_AWK
AC_PROG_LIBTOOL
AC_PROG_RANLIB

#############################################################################
# Check for some header files (TODO: Add more?)
#############################################################################
AC_HEADER_STDC
AC_CHECK_HEADERS([limits.h stdlib.h string.h unistd.h])

##############################################################################
# Check for iconv
##############################################################################
AC_CHECK_FUNCS([iconv])
AC_CHECK_HEADERS([iconv.h])
AC_CHECK_LIB([iconv],[main])

##############################################################################
# Check for pthread presence
##############################################################################
AC_CHECK_HEADERS([pthread.h],
		 [],
		 [AC_MSG_ERROR(sib-daemon needs pthread library)])
AC_CHECK_LIB([pthread],[main])

#############################################################################
# Check for piglet
#############################################################################
AC_CHECK_HEADERS([cpiglet.h],
		 [],
		 [AC_MSG_ERROR(sib-daemon needs piglet library)])
AC_CHECK_LIB([piglet],[main])

#############################################################################
# Check whether debug prints should be used
#############################################################################
AC_ARG_WITH(debug,
        AS_HELP_STRING([--with-debug],
                       [Print additional debug messages (default = no)]),
        [AC_DEFINE([WHITEBOARD_DEBUG],[1],[Print debug messages])],
        [with_debug=no]
)

AM_CONDITIONAL(DEBUG, test "x$with_debug"="xyes")

#############################################################################
# Check whether WQL should be used
#############################################################################
AC_ARG_ENABLE(wql,
        AS_HELP_STRING([--enable-wql],
                       [Should Wilbur queries (and Python) be included (default = yes)]),
[
	if test $enableval = no; then
		use_wql=no
	else
		use_wql=yes
		AC_DEFINE([WITH_WQL],[1],[Include Wilbur queries and Python])

	fi
	],
[
		use_wql=yes	
		AC_DEFINE([WITH_WQL],[1],[Include Wilbur queries and Python])

]
)

AM_CONDITIONAL(WQL, test "x$use_wql"="xyes")


##############################################################################
# Check for GNOME environment
##############################################################################
PKG_CHECK_MODULES(GNOME,
[
	glib-2.0 >= 2.8.6
	dbus-1 >= 0.61
	dbus-glib-1 >= 0.61
])

AC_SUBST(GNOME_CFLAGS)
AC_SUBST(GNOME_LIBS)

PKG_CHECK_MODULES(WHITEBOARD,
[
	libm3_parse_n_gen = 2.0-beta1
])

AC_SUBST(WHITEBOARD_CFLAGS)
AC_SUBST(WHITEBOARD_LIBS)

PKG_CHECK_MODULES(LIBSIB,
[
	libsib = 2.0-beta1
])

AC_SUBST(LIBSIB_CFLAGS)
AC_SUBST(LIBSIB_LIBS)

#PKG_CHECK_MODULES(CONIC,
#[
#	conic >= 0.13
#])

#AC_SUBST(CONIC_FLAGS)
#AC_SUBST(CONIC_LIBS)

##############################################################################
# Checks for typedefs, structures, and compiler characteristics.
##############################################################################

AC_C_CONST
AC_C_INLINE
AC_STRUCT_TM

##############################################################################
# Locale stuff
##############################################################################

AC_DEFINE_UNQUOTED([LOCALEDIR], 
		   "${runtime_localedir}", [Runtime locale catalog files path])

##############################################################################
# Check compile target to be able to disable local player in emulator
##############################################################################

case $host in
        i386-* | i686-*)
               CFLAGS="$CFLAGS -Di686"
	       USE_i686=yes
	       USE_ARM=no
               ;;
        arm-*)
               CFLAGS="$CFLAGS -DARM"
	       USE_ARM=yes
	       USE_i686=no
               ;;
esac

AM_CONDITIONAL(ARM, test "x$USE_ARM"="xyes")
AM_CONDITIONAL(i686, test "x$USE_i686"="xyes")

if test $use_wql = yes; then
   if test $USE_i686 = yes; then
      echo "using python in host: " $host
      PYTHON_VERSION=`python -V 2>&1| sed 's/\(.*\) \(.*\)/\2/'`
   else
	echo "using python2.5 in host: " $host
	PYTHON_VERSION=`python2.5 -V 2>&1| sed 's/\(.*\) \(.*\)/\2/'`
   fi

   case ${PYTHON_VERSION} in
        2.5.*)
		PYTHON_SITE_DIR=/usr/lib/python2.5/site-packages
		PYTHON_CFLAGS=`python-config --cflags`
   		PYTHON_LIBS=`python-config --ldflags`

		;;
	2.6.*)
		PYTHON_SITE_DIR=/usr/lib/python2.6/dist-packages
   		PYTHON_CFLAGS=`python2.6-config --cflags`
   		PYTHON_LIBS=`python2.6-config --ldflags`

		;;
    	*)
		AC_MSG_ERROR(Unknown python version: ${PYTHON_VERSION})
    		;;
   esac
   AC_SUBST(PYTHON_SITE_DIR)
   AC_SUBST(PYTHON_CFLAGS)
   AC_SUBST(PYTHON_LIBS)
fi



##############################################################################
# Files to create
##############################################################################

AC_OUTPUT(
	Makefile \
	include/Makefile \
	src/Makefile \
	etc/Makefile \
	python/Makefile
)

if test $use_wql = yes; then
   echo "Installing python files to: " ${PYTHON_SITE_DIR}
else
   echo "WQL disabled, not installing python files"
fi
