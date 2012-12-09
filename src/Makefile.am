bin_PROGRAMS = sibd

# Compiler flags
sibd_CFLAGS  = -Wall -I$(top_srcdir)/include -I/usr/local/include -I.
sibd_CFLAGS += @GNOME_CFLAGS@ @WHITEBOARD_CFLAGS@ @LIBSIB_CFLAGS@

if WQL
sibd_CFLAGS += @PYTHON_CFLAGS@ 
endif

# TODO: Define this in a more global and flexible way

sibd_CFLAGS += -DSIB_LIBEXECDIR=\"$(libdir)/sib/libexec\"
if DEBUG
sibd_CFLAGS += -g -O0
else
sibd_CFLAGS += -g -O2
endif


# Linker flags -- 25.5.2009 added libpiglet / jh
sibd_LDFLAGS = @GNOME_LIBS@  @WHITEBOARD_LIBS@ @LIBSIB_LIBS@ -lpiglet -lgthread-2.0

if WQL
sibd_LDFLAGS += @PYTHON_LIBS@ 
endif


AM_CFLAGS = $(sibd_CFLAGS) -g

# Put these in alphabetical order so they are easy to find.
# Leave main.c out of this list because it must not be present
# in the unit testing library build.
sources = \
	dbushandler.c \
	sib_control.c \
	sib_operations.c \
	LCTableTools.c

sibd_SOURCES = \
	main.c \
	$(sources)
