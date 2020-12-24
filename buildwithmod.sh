#!/usr/bin/env bash
set -x
set -o errexit
# compile specified module
modules=(collector receiver)

tags=""

# older version Git don't support --short !
if [ -d ".git" ];then
    #branch=`git symbolic-ref --short -q HEAD`
    branch=$(git symbolic-ref -q HEAD | awk -F'/' '{print $3;}')
    cid=$(git rev-parse HEAD)
else
    branch="unknown"
    cid="0.0"
fi
branch=$branch","$cid

baseDir=`pwd`
output=${baseDir}/bin/

# make sure we're in the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

rm -rf ${output}

GOPATH=$(pwd)
export GOPATH

#compile_line='-race'
compile_line=''
if [ -z "$DEBUG" ]; then
    DEBUG=0
fi


go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct

info="mongoshake/common.BRANCH=$branch"
# inject program information about compile
if [ $DEBUG -eq 1 ]; then
	echo "[ BUILD DEBUG ]"
	info=$info',debug'
else
	echo "[ BUILD RELEASE ]"
	info=$info",release"
fi

# golang version
goversion=$(go version | awk -F' ' '{print $3;}')
info=$info","$goversion
bigVersion=$(echo $goversion | awk -F'[o.]' '{print $2}')
midVersion=$(echo $goversion | awk -F'[o.]' '{print $3}')
if  [ $bigVersion -lt "1" -o $bigVersion -eq "1" -a $midVersion -lt "9" ]; then
    echo "go version[$goversion] must >= 1.9"
    exit 1
fi

t=$(date "+%Y-%m-%d_%H:%M:%S")
info=$info","$t

run_builder='go build -v'

goos=(linux darwin windows)
for g in "${goos[@]}"; do
    export GOOS=$g
    echo "try build goos=$g"
    if [ $g != "windows" ]; then
        build_info="$info -X mongoshake/common.SIGNALPROFILE=31 -X mongoshake/common.SIGNALSTACK=30"
    else
        build_info=$info
    fi

    cd "src/mongoshake"
    go mod tidy
    for i in "${modules[@]}" ; do
        echo "Build ""$i"

        # fetch all files in the main directory
        build_dir="mongoshake/$i/main"

        # build

#        go build -v -ldflags '-X mongoshake/common.BRANCH=unknown,0.0,debug,go1.15.6,2020-12-16_19:54:17 -X mongoshake/common.SIGNALPROFILE=31 -X mongoshake/common.SIGNALSTACK=30' '-gcflags=-N -l' -o bin/collector.linux -tags debug src/mongoshake/collector/main/collector.go src/mongoshake/collector/main/sanitize.go

        if [ $DEBUG -eq 1 ]; then
            $run_builder ${compile_line} -ldflags "-X $build_info" -gcflags='-N -l' -o "${baseDir}/bin/$i.$g" -tags "debug" $build_dir
        else
            $run_builder ${compile_line} -ldflags "-X $build_info" -o "${baseDir}/bin/$i.$g" $build_dir
        fi
        # out to bin
        # execute and show compile messages
        if [ -f ${output}/"$i" ];then
            ${output}/"$i"
        fi
    done
    cd ../../
    echo "build $g successfully!"
done
# *.sh
cp scripts/start.sh ${output}
cp scripts/stop.sh ${output}
cp scripts/mongoshake-stat ${output}
cp scripts/comparison.py ${output}


if [ "Linux" == "$(uname -s)" ];then
	# hypervisor
	gcc -Wall -O3 scripts/hypervisor.c -o ${output}/hypervisor -lpthread
elif [ "Darwin" == "$(uname -s)" ];then
	printf "\\nWARNING !!! MacOS doesn't supply hypervisor\\n"
fi
zip ${BUILD_URL}/${PACKAGE_NAME} -r bin/ collector_*
echo "成功执行"