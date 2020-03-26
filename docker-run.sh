mkdir -p sonic/store
DIR=`pwd`
docker run -d -p 1491:1491 --name sonic -v $DIR/config.cfg:/etc/sonic.cfg -v $DIR/sonic/store/:/var/lib/sonic/store/ valeriansaliou/sonic:v1.2.3

