.PHONY: build release install uninstall clean test doc reindent

build:
	jbuilder build @install --dev

release:
	jbuilder build @install

install:
	jbuilder install

uninstall:
	jbuilder uninstall

clean:
	jbuilder clean

test:
	jbuilder runtest

# requires odoc
doc:
	jbuilder build @doc

reindent:
	ocp-indent --syntax cstruct -i lib/*.ml
	ocp-indent --syntax cstruct -i lib_test/*.ml
