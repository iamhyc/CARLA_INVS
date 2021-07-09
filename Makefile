all:install

install:check-dependency

check-dependency:
	sudo apt install libxerces-c3.2 libjpeg8
    #
	conda install -c open3d-admin -c conda-forge open3d -y
	conda install scikit-learn matplotlib plyfile networkx shapely lxml -y
	conda install -c conda-forge halo -y
	pip3 install picotui
    #
	rm -rf PCDet; git clone https://github.com/iamhyc/OpenPCDet.git PCDet --depth=1
	$(MAKE) -C PCDet

check-dependency:
    # #check conda
    # #check conda environment

clean-dist:
	rm -rf raw_data/ log/
