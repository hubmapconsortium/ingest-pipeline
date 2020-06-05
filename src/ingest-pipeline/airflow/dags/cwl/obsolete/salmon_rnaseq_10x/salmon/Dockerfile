FROM mruffalo/salmon-custom:latest

WORKDIR /opt

RUN curl https://giygas.compbio.cs.cmu.edu/salmon/grch38_index.tar | tar -xf -
RUN curl -O https://giygas.compbio.cs.cmu.edu/salmon/Homo_sapiens.GRCh38.cdna.all.fa.gz.map

COPY salmon_wrapper.py /opt
