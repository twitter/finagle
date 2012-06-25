# todo: custom pic2graph

%.ps: %.pic
	cat $< | groff -e -p -Tps -P-pa0 > $@

%.pdf: %.ps
	convert -trim -crop 0x0 $< $@

%.png: %.ps
	convert -trim -crop 0x0 $< $@

all: lbstack.png filterstack.png morestack.png lbdown.png
clean:
	rm -f lbstack.png filterstack.png morestack.png lbdown.png 
