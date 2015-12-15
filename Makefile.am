#  Copyright (C) 2015 Tobias Oetiker

AUTOMAKE_OPTIONS =  foreign

MJ_CLASS = LogFetcher
MJ_SCRIPT = logfetcher

SUBDIRS = thirdparty lib etc

BIN = bin/$(MJ_SCRIPT).pl

PERLTESTS := $(shell find t -name "*.t")

THIRDPARTY_DIR := $(shell pwd)/thirdparty

EXTRA_DIST = VERSION PERL_MODULES COPYRIGHT LICENSE CHANGES AUTHORS bootstrap $(wildcard t/*.t) $(BIN) $(PERLTESTS)

YEAR := $(shell date +%Y)
DATE := $(shell date +%Y-%m-%d)

datadir = $(prefix)

dist_bin_SCRIPTS = $(BIN)

dist-hook: thirdparty/CPAN/touch
	$(PERL) -i -p -e 's/#VERSION#/$(PACKAGE_VERSION)/g;s/#YEAR#/$(YEAR)/g;s/#DATE#/$(DATE)/g;'  $(distdir)/README $(distdir)/COPYRIGHT $(distdir)/public/index.html
	$(PERL) -i -p -e 's/^our\s+\$$VERSION\s*=\s*".+?";/our \$$VERSION = "$(PACKAGE_VERSION)";/;'  $(distdir)/lib/$(MJ_CLASS).pm

install-exec-hook:
	[ "$(PERL5LIB)" == "" ] || cd "$(DESTDIR)$(exec_prefix)" && $(PERL) -i -p -e 's{.*# PERL5LIB}{use lib qw($(PERL5LIB)); # PERL5LIB}' $(BIN) || true
	cd "$(DESTDIR)$(exec_prefix)" && $(PERL) -i -p -e 's{.*# LIBDIR}{use lib qw($(libdir)); # LIBDIR}' $(BIN)
	cd "$(DESTDIR)$(exec_prefix)" && $(PERL) -i -p -e 's{^#!.*perl.*}{#!$(PERL)};' $(BIN)
	[ ! -d $(THIRDPARTY_DIR)/lib/perl5 ] || cp -fr $(THIRDPARTY_DIR)/lib/perl5/* $(DESTDIR)$(libdir)

test:
	$(PERL) "-MExtUtils::Command::MM" "-e" "test_harness(0, 'lib','thirdparty/lib/perl5')" t/*.t
