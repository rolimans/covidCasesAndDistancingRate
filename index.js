const fs = require('fs');
const moment = require('moment');
const stringSimilarity = require('string-similarity');
const neatCsv = require('neat-csv');
const { Client } = require('@elastic/elasticsearch');
const JSONStream = require("JSONStream");
const { parse } = require('path');
const client = new Client({
    node: 'https://elasticsearch-saps.saude.gov.br',
    auth: {
        username: 'user-public-notificacoes',
        password: 'Za4qNXdyQNSa9YaA'
    }
});

const toTitleCase = (str) => {
    return str.replace(
        /\w\S*/g,
        (txt) => {
            return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
        }
    );
};

const getCases = async () => {

    const results = []
    const responseQueue = []
    const municipios = JSON.parse(fs.readFileSync('municipios.json'));

    let sum = 0;

    // start things off by searching, setting a scroll timeout, and pushing
    // our first response into the queue to be processed
    const response = await client.search({
        index: 'desc-notificacoes-esusve-sp',
        // keep the search results "scrollable" for 30 seconds
        scroll: '30s',
        // for the sake of this example, we will get only one result per search
        size: 10000,
        // filter the source to only include the quote field
        //_source: ['quote'],
        body: {
            query: {
                bool: {
                    must: [
                        {
                            match: {
                                estadoTeste: "Concluído",
                            }
                        },
                        {
                            match: {
                                resultadoTeste: "Positivo",
                            }
                        },
                        {
                            terms: {
                                municipio: municipios
                            }
                        }
                    ],
                    must_not: [
                        {
                            match: {
                                evolucaoCaso: "Cancelado",
                            }
                        },
                        {
                            match: {
                                evolucaoCaso: "Ignorado",
                            }
                        }
                    ]
                }
            }
        }
    });

    responseQueue.push(response)

    while (responseQueue.length) {
        const { body } = responseQueue.shift()


        body.hits.hits.forEach(function (hit) {
            // console.log(hit._score)
            results.push(hit._source)
        })

        // check to see if we have collected all of the quotes
        if (body.hits.total.value === results.length) {
            console.log('Every quote', results)
            break
        } else {
            console.log(body.hits.total.value - results.length);
        }

        // get the next response if there are more quotes to fetch
        responseQueue.push(
            await client.scroll({
                scrollId: body._scroll_id,
                scroll: '30s'
            })
        )
    }

    const transformStream = JSONStream.stringify();
    const outputStream = fs.createWriteStream("./cases.json");
    transformStream.pipe(outputStream);
    results.forEach(transformStream.write);
    transformStream.end();

    outputStream.on(
        "finish",
        function handleFinish() {
            console.log("Done!");
        }
    );

    //    console.log(result.body.hits.hits);
};




const transformTaxaIsolamento = async () => {
    let csvFile = fs.readFileSync('Dados.csv', { encoding: 'ucs-2' }).toString().split('\n');
    csvFile.shift();
    csvFile = csvFile.join('\n');

    const data = await neatCsv(csvFile, {separator: '\t'});
    
    // const municipios = [];

    // for (let d of data) {
    //     if (d["Município1"] !== "ESTADO DE SÃO PAULO") {
    //         municipios.push(toTitleCase(d['Município1']));
    //     }
    // }

    fs.writeFileSync('taxaIsolamento.json', JSON.stringify(data), {encoding: 'utf8'});
    // fs.writeFileSync('municipios.json', JSON.stringify(municipios));
};

function isValidDate(d) {
    return d instanceof Date && !isNaN(d);
}

const processCases = () => {
    const casesStream = fs.createReadStream("cases.json");
    var stream = JSONStream.parse('*')

    const data = {};
    const errors = [];

    let sum = 0;

    stream.on('data', function (d) {

        const dateStr = d.dataInicioSintomas ? d.dataInicioSintomas : d.dataNotificacao;
        const date = new Date(dateStr);
        if (isValidDate(date)) {

            const municipio = d.municipio;
            const cleanOccurence = {};

            if (data[municipio] === undefined) {
                data[municipio] = {};
            }

            date.setHours(0);
            date.setMinutes(0);
            date.setSeconds(0);
            date.setMilliseconds(0);

            // REMOVE HOUR OF DATE

            if (data[municipio][date] === undefined) {
                data[municipio][date] = [];
            }

            cleanOccurence.id = d.source_id;
            cleanOccurence.sex = d.sexo;
            cleanOccurence.age = d.idade;
            cleanOccurence.testType = d.tipoTeste;
            cleanOccurence.symptoms = d.sintomas;
            cleanOccurence.otherSymptoms = d.outrosSintomas;
            cleanOccurence.caseConclusion = d.evolucaoCaso;

            data[municipio][date].push(cleanOccurence);
        } else {
            errors.push(d);
        }
        sum++;
        console.log(sum);
    });

    casesStream.pipe(stream);

    stream.on('end', () => {
        console.log("Erros: " + errors.length);
        console.log(errors);
        fs.writeFileSync('casesClean.json', JSON.stringify(data));
    })
    //console.log(cases);
};

const processCasesClean = async () => {
    const data = JSON.parse(fs.readFileSync('casesClean.json'));
    const lowerBound = new Date('01/01/2020');
    const upperBound = new Date();

    const newData = {};

    for (let cidade in data) {
        let totalDaysInBound = 0;
        //console.log('\n' + cidade + ":");
        //console.log('------------------------------\n');
        const ordered = {};
        Object.keys(data[cidade]).sort((a, b) => {
            const dateA = new Date(a);
            const dateB = new Date(b);
            return dateA.getTime() > dateB.getTime() ? 1 : -1;
        }).forEach(function (key) {
            ordered[key] = data[cidade][key];
        });
        const possibility = {};
        for (let dia in ordered) {
            let date = new Date(dia);
            if (date < lowerBound || date > upperBound) {
                continue;
            }
            possibility[dia] = ordered[dia];
            // console.log(date.toLocaleDateString() + ": " + ordered[dia].length);
            totalDaysInBound++;
        }
        if (totalDaysInBound >= 100) {
            newData[cidade] = possibility;
        }//else{
        //console.log(cidade);
        //}
    }

    fs.writeFileSync('casesCleanBounded.json', JSON.stringify(newData));
}

const processTaxaIsolamento = () => {
    const cidades = JSON.parse(fs.readFileSync('municipios.json'));
    const isol = JSON.parse(fs.readFileSync('taxaIsolamento.json'));
    const data = {};
    //console.log(Object.keys(isol[0]));

    for (let i of isol) {
        if (i['Município1'] === 'ESTADO DE SÃO PAULO') {
            continue;
        }
        const matches = stringSimilarity.findBestMatch(toTitleCase(i['Município1']), cidades);
        const cidade = matches.bestMatch.target;
        data[cidade] = {};
        data[cidade]['population'] = i['População estimada (2020)'];
        data[cidade]['cityCodeIBGE'] = i['Código Município IBGE'];
        data[cidade]['distancingRate'] = {};
        for (let day in i) {
            if (!day.includes('/')) {
                continue;
            }
            const splitted = day.split('/');
            const correctDayString = splitted[1] + '/' + splitted[0] + '/2020';
            const date = new Date(correctDayString);
            const rate = parseInt(i[day].replace('%', '')) / 100;
            data[cidade]['distancingRate'][date] = rate;
        }
    }

    fs.writeFileSync("taxaIsolamentoParsed.json", JSON.stringify(data));
};

const getMedia = (rates) => {
    let sum = 0;
    let total = 0;
    for (let day in rates) {
        if (rates[day] !== null) {
            sum += rates[day];
            total++;
        }
    }
    return sum / total;
};

const removeNaN = () => {
    const isol = JSON.parse(fs.readFileSync('taxaIsolamentoParsed.json'));

    for (let cidade in isol) {
        for (let day in isol[cidade].distancingRate) {
            if (isol[cidade].distancingRate[day] === null) {
                isol[cidade].distancingRate[day] = getMedia(isol[cidade].distancingRate);
            }
        }
    }

    fs.writeFileSync('taxaIsolamentoClean.json', JSON.stringify(isol));
}

const getInsightsOfWeek = (cases, week) => {
    let tests = {};
    let weekDayWithMostCases;
    let weekDayWithMostCasesN = 0;
    let feminino = 0;
    let masculino = 0;
    let sumOfAges = 0;
    let nOfAges = 0;

    for (let day in cases) {
        const date = moment(new Date(day));
        if (date.week() === week) {
            if (cases[day].length > weekDayWithMostCasesN) {
                weekDayWithMostCasesN = cases[day].length;
                weekDayWithMostCases = date.weekday();
            }
            for (let c of cases[day]) {
                if (tests[c.testType]) {
                    tests[c.testType]++;
                } else {
                    tests[c.testType] = 1;
                }
                if (c.sex === "Masculino") {
                    masculino++;
                } else if (c.sex === "Feminino") {
                    feminino++;
                }
                sumOfAges += c.age;
                nOfAges++;
            }
        }
    }

    const mostCommonTest = Object.keys(tests).sort((a, b) => {
        return tests[a] > tests[b] ? -1 : 1;
    })[0];

    return {
        averageAge: Math.round(sumOfAges / nOfAges),
        mostInfectedSex: feminino === masculino ? 'E' : (feminino > masculino ? 'F' : 'M'),
        weekDayWithMostCases,
        mostCommonTest
    }
};

const processCasesCleanBounded = async () => {
    const cases = JSON.parse(fs.readFileSync('casesCleanBounded.json'));
    const isol = JSON.parse(fs.readFileSync('taxaIsolamentoClean.json'));

    const weekGrowthInCases = {};

    const data = {};
    const frame = [];

    let currWeek;
    let summedCasesWeek;
    let totalCasesUntilNow;

    let lastWeekCases;
    let dayOfFirstCase;

    for (let cidade in cases) {
        dayOfFirstCase = null;
        lastWeekCases = 0;
        data[cidade] = {};
        totalCasesUntilNow = 0;
        weekGrowthInCases[cidade] = {};
        currWeek = -1;
        summedCasesWeek = 0;
        lastWeekPct = 0;

        for (let dia in cases[cidade]) {
            const date = moment(new Date(dia));
            if (!dayOfFirstCase) {
                dayOfFirstCase = date;
            }
            totalCasesUntilNow += cases[cidade][dia].length;
            summedCasesWeek += cases[cidade][dia].length;
            if (currWeek !== date.week()) {
                if (currWeek !== -1) {
                    data[cidade][currWeek] = { ...getInsightsOfWeek(cases[cidade], currWeek) };
                    data[cidade][currWeek].daysSinceFirstCase = date.diff(dayOfFirstCase, 'days');
                    if (lastWeekCases !== 0) {
                        data[cidade][currWeek].lastWeekCases = lastWeekCases;
                        data[cidade][currWeek].growthFactor = summedCasesWeek / lastWeekCases;
                        if (data[cidade][currWeek].growthFactor >= 1.1) {
                            data[cidade][currWeek].growthFactorIncreased = "Y";
                        } else {
                            data[cidade][currWeek].growthFactorIncreased = "N";
                        }
                    } else {
                        data[cidade][currWeek].lastWeekCases = null;
                        data[cidade][currWeek].growthFactor = null;
                        data[cidade][currWeek].growthFactorIncreased = null;
                    }
                    data[cidade][currWeek].percentageInfected = totalCasesUntilNow / isol[cidade].population;
                    if (lastWeekPct) {
                        data[cidade][currWeek].percentageInfectedLastWeek = lastWeekPct;
                    } else {
                        data[cidade][currWeek].percentageInfectedLastWeek = null;
                    }
                    data[cidade][currWeek].newCases = summedCasesWeek;
                    //console.log(currWeek);
                    //console.log(data[cidade][currWeek]);
                    lastWeekPct = totalCasesUntilNow / isol[cidade].population;
                    lastWeekCases = summedCasesWeek;
                    summedCasesWeek = 0;
                }
                currWeek = date.week();
            }
        }

        currWeek = -1;
        let sumOfRates = 0;
        let numOfRates = 0;
        let lastWeekDistancingRate = 0;
        let lastTwoWeeks = 0;
        let lastThreeWeeks = 0;

        for (let dia in isol[cidade].distancingRate) {
            const date = moment(new Date(dia));
            sumOfRates += isol[cidade].distancingRate[dia];
            numOfRates++;
            if (currWeek !== date.week()) {
                if (currWeek !== -1) {
                    const avg = sumOfRates / numOfRates;
                    if (data[cidade][currWeek] !== undefined) {
                        data[cidade][currWeek].averageDistancingRate = avg;
                        if (lastWeekDistancingRate !== 0) {
                            data[cidade][currWeek].averageDistancingRate1 = lastWeekDistancingRate;
                        } else {
                            data[cidade][currWeek].averageDistancingRate1 = null;
                        }
                        if (lastTwoWeeks !== 0) {
                            data[cidade][currWeek].averageDistancingRate2 = lastTwoWeeks;
                            data[cidade][currWeek].distancingRateDifference1 = lastWeekDistancingRate - lastTwoWeeks;
                        } else {
                            data[cidade][currWeek].averageDistancingRate2 = null;
                            data[cidade][currWeek].distancingRateDifference1 = null;
                        }
                        if (lastThreeWeeks !== 0) {
                            data[cidade][currWeek].averageDistancingRate3 = lastThreeWeeks;
                            data[cidade][currWeek].distancingRateDifference2 = ((lastWeekDistancingRate - lastThreeWeeks) + (lastWeekDistancingRate - lastTwoWeeks)) / 2;
                        } else {
                            data[cidade][currWeek].averageDistancingRate3 = null;
                            data[cidade][currWeek].distancingRateDifference2 = null;
                        }
                    }
                    lastThreeWeeks = lastTwoWeeks;
                    lastTwoWeeks = lastWeekDistancingRate;
                    lastWeekDistancingRate = avg;
                    sumOfRates = numOfRates = 0;
                    //console.log(data[cidade][currWeek]);
                }
                currWeek = date.week();
            }
        }
    }

    for (let cidade in data) {
        for (let week in data[cidade]) {
            data[cidade][week].weekOfYear = week;
            data[cidade][week].city = cidade;
            frame.push(data[cidade][week]);
        }
    }

    console.log(frame.length);

    fs.writeFileSync("database.json", JSON.stringify(frame));
}

// processCasesClean()