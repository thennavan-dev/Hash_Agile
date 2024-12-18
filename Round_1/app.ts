function rotateArray(arr: number[], k: number): number[] {
    if (arr.length === 0 || k === 0) {
        return arr;
    }

    const length = arr.length;
    k = k % length;

    const lastPart = arr.slice(length - k);

    const firstPart = arr.slice(0, length - k);

    const result = lastPart.concat(firstPart);

    return result;
}

const arr = [1, 2, 3, 4, 5];
const k = 2;
const rotatedArray = rotateArray(arr, k);
console.log(rotatedArray);
