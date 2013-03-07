class FindContinuousNumber {
    static def main(args){
        def array = [1,2,3,5,6,7,8,11,12,13,14,15];
        int maxCn = 0, maxStart = 0, i = 0;
        while (i < array.size()){
            int n = 0, j = i;
            while(++i < array.size() && array[i-1] + 1 == array[i]) ++n;
            maxCn = maxCn > n? maxCn : n;
            maxStart = maxCn > n? maxStart : j;
        }
        println array[maxStart .. maxStart + maxCn];
    }
}