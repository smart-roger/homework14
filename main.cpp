#include <iostream>
#include <cstring>
#include <exception>
#include <mutex>
#include <vector>
#include <string>
#include <memory>
#include <list>
#include <thread>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <functional>

//  Контейнеры для передачи данных
using typeResultContainer = std::vector<std::string>  ;
using typeResultContainerPtr = std::shared_ptr<typeResultContainer>;

using funcMapper = std::function<void(std::string, typeResultContainerPtr)>;
using funcReducer = std::function<void(typeResultContainerPtr&, std::shared_ptr<std::ofstream>)>;

//  Структура задачи для маппера
struct mapTask{
    std::shared_ptr<std::thread> threadTask;
    typeResultContainerPtr ptrResult;
};

//  Структура задачи для редюсера
struct reducerTask{
    std::shared_ptr<std::thread> threadTask;
    std::shared_ptr<std::ofstream> fileOut;
};

//  Функция определения общего префикса у нескольких строк
size_t commonPrefix(std::string& left, std::string& right){
    size_t len = std::min(left.size(), right.size());
    size_t prefix;
    for(prefix=0; prefix<len; ++prefix){
        if(left.at(prefix)!=right.at(prefix))
            return prefix;
    }
    return prefix;
}

//  Слияние и сортировка обработанных данных
typeResultContainer mergeSort(std::vector<typeResultContainerPtr> inputs){
    typeResultContainer    result;

    std::for_each(inputs.begin(), inputs.end(), [&result](auto mapper_result){
                        std::copy(mapper_result->begin(), mapper_result->end(), std::back_inserter(result));
                  });
    std::sort(result.begin(), result.end());
    return result;
}

//  Определение ключа для выбора редюсера
unsigned char getKey(std::string value, int countReducers){
    std::hash<std::string> hash_fn;
	if(value.empty())
        return 0;
    else return hash_fn(value) % countReducers;
}

//  Мютекс для считывания из файлов в нескольких потоках
std::mutex  mutexFile;

void mapper(std::string line, typeResultContainerPtr result){
	if (nullptr == result)
		return;

	for(size_t idx=1; idx <line.length(); ++idx){
		result->push_back(line.substr(0,idx));	
		//std::cout << line.substr(0,idx) << std::endl;
	}
}

//  Функция для маппера. Считывает данные в память и затем разбивает их на строки.
void readFunction(std::ifstream& fileIn,
                    std::iostream::pos_type startPos,
                    std::iostream::pos_type stopPos,
					typeResultContainerPtr result,
					funcMapper mapper
                    ){
try{
    size_t length(stopPos - startPos);
    std::vector<char> buffer;
    buffer.resize(length+1);

    //  Чтение куска файла в память
    std::lock_guard<std::mutex> lock(mutexFile);
    {
        fileIn.seekg(startPos, std::ios_base::beg);
        fileIn.read(buffer.data(), length);
    }
    buffer[length]=0;

    //  Разбираем считанный кусок на строки
    char* ptrBuffer(buffer.data());
    char* ptrLine(nullptr);

    ptrLine = strtok(ptrBuffer, "\n");
    while(ptrLine != nullptr){
        mapper(ptrLine, result);
        ptrLine = strtok(nullptr, "\n");
    }
} catch(std::exception& e){
    std::cout << "Error in mapper. " << e.what() << std::endl;
}
}

//  Функция для редюсера
void reduceFunction (typeResultContainerPtr& input, std::shared_ptr<std::ofstream> out){
    if (input->empty()) return;

    size_t minimumPrefix(1);
	size_t counter(0);
	
    std::string current("");
    for (std::string prefix : *input) {
		if (minimumPrefix <= prefix.length() ){
			if(current == prefix )
			{
				minimumPrefix = prefix.length()+1;
			}else{
				current = prefix;
			}
		}
	}

    (*out) << "Minimum prefix: " << minimumPrefix << std::endl;
	//std::cout << "Minimum prefix: " << minimumPrefix << std::endl;
    //(*out) << "Reducer data:" << std::endl;
    //std::for_each(input->begin(), input->end(), [&out](auto line){
    //            (*out) << line << std::endl;});
/*
    std::cout << "Minimum prefix: " << minimumPrefix << std::endl;
    std::cout << "Reducer data:" << std::endl;
    std::for_each(input->begin(), input->end(), [&out](auto line){
                std::cout << line << std::endl;});
*/
}

int main(int argc, char** argv)
{
//  Получаем и проверяем входные параметры
    if(argc<3){
        std::cout << "Wrong arguments. Usage: yamr <src> <mnum> <rnum>" << std::endl;
        return -1;
    };

    std::ifstream fileInput(argv[1]);
    if(!fileInput.good()){
        std::cout << "Problems with file " << argv[0] << std::endl;
        return -2;
    }

    int countMaps = std::stoi(argv[2]);
    if(countMaps<=0){
        std::cout << "mnum should be > 0" << std::endl;
        return -3;
    }

    int countReducers = std::stoi(argv[3]);
    if(countReducers<=0){
        std::cout << "rnum should be > 0" << std::endl;
        return -4;
    }
//  Делаем разбиение файла на части

//  Получаем размер файла и определяем размер пачки
    fileInput.seekg(0,std::ios_base::end);
    auto pos_end = fileInput.tellg();
    auto part = pos_end/countMaps;
    std::vector<std::iostream::pos_type>    borders;
    borders.reserve(countMaps);

    //  Разбиваем на части
    std::cout << "splitting" << std::endl;
    fileInput.seekg(part, std::ios_base::beg);
    auto pos=part;
    while( (pos < pos_end) && (pos>=0)){
        fileInput.seekg(pos, std::ios_base::beg);
        //  И выравниваем по строке
        char symbol(0);
        while(symbol != '\n' && !fileInput.fail())
            fileInput.read(&symbol, 1);

        pos=fileInput.tellg();

        if(pos>=0){
            borders.push_back(pos);
            pos+=part;
        }
    }
    borders.push_back(pos_end);

    //  Формируем задачи для мапперов
    std::cout << "Mapping" << std::endl;
    std::list<mapTask> taskForMapperAll;

    std::iostream::pos_type begin(0), end(0);
    auto iterBorder = borders.cbegin();

    for(auto numTask=0;
            (numTask< countMaps) && (iterBorder!=borders.cend());
            ++numTask, ++iterBorder){
        end = *iterBorder;
        mapTask newTask;
        newTask.ptrResult = std::make_shared<typeResultContainer>();
        newTask.threadTask = std::make_shared<std::thread> (
                                readFunction,
                                std::ref(fileInput),
                                begin, end,
								newTask.ptrResult,
                                mapper);
        taskForMapperAll.push_back(newTask);
        begin = end;
    }

    //  Ожидаем окончания работы мапперов и сортируем результат
    std::vector<typeResultContainerPtr> vecResult;
    std::for_each(taskForMapperAll.begin(), taskForMapperAll.end(),
                  [&vecResult](auto task){
                        task.threadTask->join();
                        typeResultContainerPtr  result(task.ptrResult);
                        std::sort(result->begin(),
                                  result->end());
                        vecResult.push_back(result);
                    });

//  Делаем слияние и сортировку для результатов
    std::vector<std::string> fromMappers = mergeSort(vecResult);

//  Формируем задачи для редюсеров
    std::vector<typeResultContainerPtr> forReducers;
    for(auto num=0; num<countReducers; ++num)
        forReducers.push_back(std::make_shared<typeResultContainer>());

//  Распределяем результаты по редюсерам
    std::for_each(fromMappers.begin(), fromMappers.end(),
                  [&forReducers, countReducers](auto line){
                    unsigned char key(getKey(line, countReducers));
                    forReducers[key]->push_back(line);
                  });

//  Запускаем редюсеры
    std::cout << "Reduce" << std::endl;
    std::list<reducerTask>  reducers;
    for(auto numTask=0;
            (numTask< countReducers);
            ++numTask){
        //  Готовим файл для вывода
        std::string fileOutName ("reducer_"+std::to_string(numTask + 1)+".out");

        //  Формируем задачу и запускаем
        reducerTask newTask;
        newTask.fileOut = std::make_shared<std::ofstream>(fileOutName);
        newTask.threadTask = std::make_shared<std::thread> (
                        reduceFunction,
                        std::ref(forReducers[numTask]),
                        newTask.fileOut);
        reducers.push_back(newTask);
    }

    //  Ожидаем завершения редюсеров
    std::for_each(reducers.begin(), reducers.end(),
                  [](auto task){
                        task.threadTask->join();
                  });
    reducers.clear();

    std::cout << "Finished" << std::endl;

    return 0;
}
