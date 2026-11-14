const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../app.js'), 'utf8');
function loadPrintHelpers(recipe) {
    const names = ['resolveLotCostDetails', 'getLotSlipPrintModel', 'buildDualLotSlipHtml'];
    const functions = names.map(name => {
        const start = source.indexOf(`function ${name}(`);
        assert.ok(start >= 0);
        const next = source.slice(start + 1).search(/\n(?:async )?function /);
        return source.slice(start, next < 0 ? undefined : start + 1 + next);
    });
    const context = vm.createContext({
        KapadState: { recipes: recipe ? [recipe] : [] },
        AppState: { currentCompany: 'company1', designPrices: {} },
        DESIGN_PRICE_SCOPE: 'global',
        getBillBrandTitle: () => 'KRIMAA',
        formatLotDateTime: () => '14/11/2026 12:21',
        esc: text => String(text).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;')
    });
    vm.runInContext(functions.join('\n'), context);
    return context;
}

const recipe = {
    designNo: '1018', silaiPrice: 75,
    materials: [{ name: 'vichitra', metersPerPc: 5.5, ratePerMeter: 32 }],
    embroidery: [{ name: 'bodi', metersPerPc: .51, ratePerMeter: 45 }],
    jobCosts: [{ name: 'Cutting', qtyPerPc: .5, rate: 4, amountPerPc: 2 }]
};
const lot = {
    designNo: '1018', lotNumber: 'TEST-LOT', companyId: 'company1', targetPcs: 100,
    fabricGiven: [{ name: 'vichitra', meters: 550, ratePerMeter: 32 }, { name: 'bodi', meters: 51, ratePerMeter: 45 }],
    jobCosts: recipe.jobCosts, totalMetersGiven: 601, silaiPrice: 75
};

test('legacy combined rows print separately and retain reconciled costs', () => {
    const { getLotSlipPrintModel } = loadPrintHelpers(recipe);
    const model = getLotSlipPrintModel(lot);
    assert.equal(model.sections[0].totalCost, 17600);
    assert.equal(model.sections[1].totalCost, 2295);
    assert.equal(model.sections[2].totalCost, 200);
    assert.equal(model.sections[2].rows[0].quantity, 50);
    assert.equal(model.sections[2].rows[0].rate, 4);
    assert.equal(model.sections.reduce((sum, group) => sum + group.totalCost, 0) + model.cost.silaiPrice * 100, model.cost.totalLotCost);
});

test('explicit item categories survive duplicate names and missing recipes', () => {
    const { getLotSlipPrintModel } = loadPrintHelpers(null);
    const model = getLotSlipPrintModel({ ...lot, embroidery: [{ name: 'Same name' }], fabricGiven: [
        { name: 'Same name', meters: 10, ratePerMeter: 2, type: 'material' },
        { name: 'Same name', meters: 4, ratePerMeter: 5, type: 'embroidery' }
    ] });
    assert.equal(model.sections[0].totalCost, 20);
    assert.equal(model.sections[1].totalCost, 20);
});

test('both copies contain three distinct colored tables; floor remarks exclude pricing', () => {
    const { buildDualLotSlipHtml } = loadPrintHelpers(recipe);
    const html = buildDualLotSlipHtml(lot);
    for (const group of ['materials', 'embroidery', 'jobwork']) {
        assert.equal(html.split(`data-consumption="${group}"`).length - 1, 2);
    }
    for (const color of ['rgb(29,78,216)', 'rgb(109,40,217)', 'rgb(180,83,9)']) assert.ok(html.includes(color));
    const floor = html.split('FLOOR / MAAL COPY - WITH GOODS')[1];
    assert.ok(floor.includes('Remark'));
    assert.ok(!floor.includes('₹'));
    assert.ok(!floor.includes('TOTAL LOT EXPENSE'));
});
