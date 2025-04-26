#include <iostream>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <sstream>
#include <cuda_runtime.h>

using namespace std;

// �n�̌����f�[�^���i�[����\����
struct Horse {
    string PrimaryKey;
    string Sire; // ��
    string Dam;  // ��
};

// �����f�[�^�iCPU���j
unordered_map<string, Horse> horses;
vector<string> horseNames;
vector<double> bloodCache;

// GPU�������p�|�C���^
double* d_bloodCache;

// CSV�t�@�C�����猌���f�[�^��ǂݍ���
void loadCSV(const string& filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "�G���[: �t�@�C�� " << filename << " ���J���܂���ł����B" << endl;
        exit(EXIT_FAILURE);
    }

    string line;
    getline(file, line); // �w�b�_�[���X�L�b�v

    while (getline(file, line)) {
        stringstream ss(line);
        string PrimaryKey, Sire, Dam;
        getline(ss, PrimaryKey, ',');
        getline(ss, Sire, ',');
        getline(ss, Dam, ',');

        horses[PrimaryKey] = { PrimaryKey, Sire, Dam };
        horseNames.push_back(PrimaryKey);
    }
    file.close();
    cout << "�����f�[�^�ǂݍ��݊����B�n�̐�: " << horses.size() << endl;
}

// GPU�J�[�l���i����v�Z�j
__global__ void computeBloodPercentageKernel(double* d_bloodCache, int numHorses) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < numHorses) {
        for (int j = 0; j < numHorses; ++j) {
            // GPU���Ō��ʌv�Z�i�V���v���ȗ�j
            d_bloodCache[i * numHorses + j] = (i == j) ? 1.0 : 0.5; // ���̒l
        }
    }
}

// GPU�Ō��ʌv�Z�����s
void computeBloodPercentageGPU(int numHorses) {
    size_t size = numHorses * numHorses * sizeof(double);

    // GPU���������m��
    cudaMalloc((void**)&d_bloodCache, size);
    cudaMemset(d_bloodCache, 0, size);

    // CUDA�J�[�l�������s
    int threadsPerBlock = 256;
    int blocksPerGrid = (numHorses + threadsPerBlock - 1) / threadsPerBlock;
    computeBloodPercentageKernel << <blocksPerGrid, threadsPerBlock >> > (d_bloodCache, numHorses);

    // �v�Z���ʂ��擾
    cudaMemcpy(bloodCache.data(), d_bloodCache, size, cudaMemcpyDeviceToHost);

    // GPU�����������
    cudaFree(d_bloodCache);
}

// CSV�t�@�C���Ɍ��ʃf�[�^�������o��
void saveCSV(const string& filename, int numHorses) {
    ofstream file(filename);
    if (!file.is_open()) {
        cerr << "�G���[: �t�@�C�� " << filename << " ���J���܂���ł����B" << endl;
        exit(EXIT_FAILURE);
    }

    // �w�b�_�[
    file << ",";
    for (const auto& name : horseNames) {
        file << name << ",";
    }
    file << "\n";

    // �e�n�̌��ʃf�[�^
    for (int i = 0; i < numHorses; ++i) {
        file << horseNames[i] << ",";
        for (int j = 0; j < numHorses; ++j) {
            file << bloodCache[i * numHorses + j] << ",";
        }
        file << "\n";
    }

    file.close();
    cout << "���ʃf�[�^�� " << filename << " �ɕۑ����܂����B" << endl;
}

int main() {
    string inputFile = "bloodline.csv";  // ���̓t�@�C��
    string outputFile = "blood_percentage.csv"; // �o�̓t�@�C��

    cout << "CSV�f�[�^�����[�h�J�n..." << endl;
    loadCSV(inputFile);
    int numHorses = horseNames.size();
    bloodCache.resize(numHorses * numHorses);

    cout << "GPU�Ō��ʌv�Z�J�n..." << endl;
    computeBloodPercentageGPU(numHorses);
    cout << "GPU�ł̌��ʌv�Z�����I" << endl;

    cout << "�v�Z���ʂ�CSV�ɕۑ�..." << endl;
    saveCSV(outputFile, numHorses);

    cout << "���ʌv�Z�����B���ʂ� " << outputFile << " �ɕۑ����܂����B" << endl;
    return 0;
}
