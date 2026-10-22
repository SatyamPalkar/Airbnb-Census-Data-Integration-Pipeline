"use strict";
const $ = id => document.getElementById(id);
const money = value => new Intl.NumberFormat('en-AU', { style: 'currency', currency: 'AUD', maximumFractionDigits: 0 }).format(value);
const integer = value => new Intl.NumberFormat('en-AU').format(value);
const monthName = value => new Date(`${value}-01T00:00:00Z`).toLocaleDateString('en-AU', { month: 'short', year: 'numeric', timeZone: 'UTC' });
const escape = text => String(text).replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
let dataset;
let currentTable = [];

function aggregate(rows) {
  const result = { count: 0, total: 0, available: 0, availableCount: 0, census: 0 };
  for (const row of rows) {
    result.count += row.listing_count; result.total += row.sum_price;
    result.available += row.sum_availability; result.availableCount += row.availability_count;
    if (row.median_age !== null) result.census += row.listing_count;
  }
  result.mean = result.count ? result.total / result.count : null;
  return result;
}

function byLga(rows) {
  const groups = new Map();
  for (const row of rows) {
    if (!groups.has(row.lga_code)) groups.set(row.lga_code, []);
    groups.get(row.lga_code).push(row);
  }
  return [...groups.entries()].map(([code, items]) => ({ code, name: items[0].lga_name,
    age: items[0].median_age, rent: items[0].median_weekly_rent, ...aggregate(items) })).sort((a,b) => b.mean - a.mean);
}

function svg(container, body, label, height = 260) {
  $(container).innerHTML = `<svg viewBox="0 0 600 ${height}" role="img" aria-label="${escape(label)}">${body}</svg>`;
}

function trend(rows) {
  const points = dataset.months.map(month => ({ month, ...aggregate(rows.filter(r => r.month === month)) }));
  const valid = points.filter(p => p.count);
  if (!valid.length) { $('trend').textContent = 'No trend observations.'; return; }
  const max = Math.max(...valid.map(p => p.mean)) * 1.15;
  const x = index => 55 + index * 510 / Math.max(points.length - 1, 1);
  const y = value => 210 - value * 180 / max;
  let body = [0, .5, 1].map(f => `<line x1="55" x2="565" y1="${y(max*f)}" y2="${y(max*f)}" stroke="#d8dcd0"/><text x="45" y="${y(max*f)+4}" text-anchor="end" font-size="11" fill="#6b746a">${Math.round(max*f)}</text>`).join('');
  let previous = null;
  for (const [index, p] of points.entries()) {
    if (!p.count) { previous = null; continue; }
    if (previous !== null) body += `<line x1="${x(previous)}" y1="${y(points[previous].mean)}" x2="${x(index)}" y2="${y(p.mean)}" stroke="#32674f" stroke-width="3"/>`;
    body += `<circle cx="${x(index)}" cy="${y(p.mean)}" r="4" fill="#32674f"><title>${monthName(p.month)}: ${money(p.mean)}, ${integer(p.count)} records</title></circle>`;
    if (index % 3 === 0 || index === points.length-1) body += `<text x="${x(index)}" y="235" text-anchor="middle" font-size="10" fill="#6b746a">${monthName(p.month)}</text>`;
    previous = index;
  }
  svg('trend', body, 'Mean asking price over all months for the selected area and room type');
}

function ranking(regions) {
  $('ranking').replaceChildren();
  if (!regions.length) { $('ranking').textContent = 'No LGAs meet the selected minimum record count.'; return; }
  const max = Math.max(...regions.map(r => r.mean));
  for (const region of regions.slice(0,8)) {
    const button = document.createElement('button'); button.type = 'button'; button.className = 'rank-row';
    button.setAttribute('aria-label', `Explore ${region.name}: ${money(region.mean)} mean nightly price`);
    button.innerHTML = `<span class="rank-name">${escape(region.name)}</span><span class="bar-track"><i class="bar-fill" style="width:${region.mean/max*100}%"></i></span><span class="rank-value">${money(region.mean)}</span>`;
    button.onclick = () => { $('lga').value = region.code; render(); };
    $('ranking').append(button);
  }
}

function scatter(regions) {
  const points = regions.filter(r => r.age !== null);
  if (!points.length) { $('scatter').textContent = 'No eligible Census-linked LGAs.'; $('correlation').textContent = ''; return; }
  const minAge = Math.min(...points.map(r => r.age))-2, maxAge = Math.max(...points.map(r => r.age))+2;
  const maxPrice = Math.max(...points.map(r => r.mean))*1.15;
  const x = age => 60 + (age-minAge)/(maxAge-minAge)*490;
  const y = price => 210-price/maxPrice*170;
  let body = `<line x1="60" x2="560" y1="210" y2="210" stroke="#b7c1b1"/><line x1="60" x2="60" y1="30" y2="210" stroke="#b7c1b1"/>`;
  for (let i=0;i<3;i++) { const age=minAge+(maxAge-minAge)*i/2; body+=`<text x="${x(age)}" y="230" text-anchor="middle" font-size="11" fill="#6b746a">${age.toFixed(0)} yrs</text>`; const price=maxPrice*i/2; body+=`<text x="50" y="${y(price)+4}" text-anchor="end" font-size="11" fill="#6b746a">$${Math.round(price)}</text>`; }
  body += points.map(r => `<circle cx="${x(r.age)}" cy="${y(r.mean)}" r="${Math.min(15,4+Math.sqrt(r.count)/20)}" fill="#32674f" opacity=".65"><title>${escape(r.name)}: age ${r.age}, ${money(r.mean)}, ${integer(r.count)} records</title></circle>`).join('');
  svg('scatter', body, 'LGA mean asking price against 2016 median resident age; bubble size reflects record count');
  const mx=points.reduce((s,r)=>s+r.age,0)/points.length, my=points.reduce((s,r)=>s+r.mean,0)/points.length;
  const cov=points.reduce((s,r)=>s+(r.age-mx)*(r.mean-my),0), dx=points.reduce((s,r)=>s+(r.age-mx)**2,0),dy=points.reduce((s,r)=>s+(r.mean-my)**2,0);
  $('correlation').textContent = points.length>=3 && dx>0 && dy>0 ? `Unweighted LGA-level Pearson r = ${(cov/Math.sqrt(dx*dy)).toFixed(3)} across ${points.length} LGAs. Association does not establish causation.` : 'At least three eligible LGAs with varying values are needed to show a correlation.';
}

function insight(headline, text) {
  const article=document.createElement('article'), strong=document.createElement('strong'), p=document.createElement('p');
  strong.textContent=headline; p.textContent=text;article.append(strong,p);$('insights').append(article);
}

function render() {
  const month=$('month').value, code=$('lga').value, room=$('room').value;
  const minimum = Math.max(1, Math.min(1000000, Number($('minimum').value) || 1));
  const areaRows=dataset.rows.filter(r=>(code==='all'||r.lga_code===code)&&(room==='all'||r.room_type===room));
  const selected=areaRows.filter(r=>month==='all'||r.month===month);
  const total=aggregate(selected);
  currentTable=byLga(selected);
  const eligible=currentTable.filter(r=>r.count>=minimum&&r.code!=='UNMAPPED');
  $('average').textContent=total.mean===null?'—':money(total.mean);
  $('records').textContent=integer(total.count);$('regions').textContent=integer(currentTable.length);
  $('coverage').textContent=total.count?`${(total.census/total.count*100).toFixed(1)}%`:'—';
  $('record-note').textContent=month==='all'?'Repeated observations across 12 months':'One observation per listing this month';
  $('empty').hidden=total.count>0;$('export').disabled=!total.count;
  $('load-status').textContent=`${month==='all'?'All snapshots':monthName(month)} · ${code==='all'?'All LGAs':$('lga').selectedOptions[0].textContent} · ${room==='all'?'All room types':room}`;
  trend(areaRows);ranking(eligible);scatter(eligible);
  $('table-body').replaceChildren();
  for(const r of currentTable) {
    const tr=document.createElement('tr');
    for(const text of [r.name,integer(r.count),money(r.mean),r.availableCount?(r.available/r.availableCount).toFixed(1):'—',r.age??'—',r.rent===null?'—':money(r.rent)]) { const td=document.createElement('td');td.textContent=text;tr.append(td); }
    $('table-body').append(tr);
  }
  $('table-count').textContent=`${currentTable.length} LGAs`;
  $('insights').replaceChildren();
  if(eligible.length>=2) { const high=eligible[0], low=eligible[eligible.length-1]; insight(`${(high.mean/low.mean).toFixed(1)}× price spread`,`${high.name} has the highest eligible mean asking price (${money(high.mean)}), compared with ${low.name} (${money(low.mean)}). This reflects observed listing mix.`); }
  else insight(`${eligible.length} eligible LGA${eligible.length===1?'':'s'}`,'A market comparison needs at least two areas meeting the minimum record count. Broaden the filters to compare more areas.');
  const entire=aggregate(selected.filter(r=>r.room_type==='Entire home/apt')), privateRoom=aggregate(selected.filter(r=>r.room_type==='Private room'));
  if(entire.count&&privateRoom.count) insight(`${(entire.mean/privateRoom.mean).toFixed(1)}× room-type difference`,`Entire-home asking prices average ${money(entire.mean)}; private rooms average ${money(privateRoom.mean)}. Locations, sizes and property mix differ.`);
  else insight(`${integer(total.count)} records in view`,'Record counts describe listing snapshots, not guests, bookings or unique properties across multiple months.');
  insight(total.availableCount?`${(total.available/total.availableCount).toFixed(1)} days available`:'Availability unavailable','Mean reported availability within the next 30 days. A blocked or unavailable date is not evidence of a booking; do not interpret this as occupancy.');
}

$('filters').addEventListener('submit',event=>event.preventDefault());
for(const id of ['month','lga','room','minimum']) $(id).addEventListener('change',()=>{if(dataset)render();});
$('export').onclick=()=>{
  const cells=value=>`"${String(value??'').replaceAll('"','""')}"`;
  const rows=[['snapshot_filter','room_type_filter','LGA','listing_month_records','mean_asking_price_aud','mean_days_available_30','census_2016_median_age','census_2016_median_weekly_rent_aud'],...currentTable.map(r=>[$('month').value,$('room').value,r.name,r.count,r.mean,r.availableCount?r.available/r.availableCount:'',r.age,r.rent])];
  const blob=new Blob([rows.map(row=>row.map(cells).join(',')).join('\r\n')],{type:'text/csv;charset=utf-8'}),url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=`rental-atlas-${$('month').value}.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
};

async function init(){
  try{
    const response=await fetch('./data/market.json');if(!response.ok)throw new Error('Aggregate dataset could not load.');
    dataset=await response.json();if(dataset.schema_version!==1||!Array.isArray(dataset.rows)||!dataset.months.length)throw new Error('Unsupported aggregate dataset.');
    const option=(select,value,label)=>{const o=document.createElement('option');o.value=value;o.textContent=label;select.append(o);};
    option($('month'),'all','All months (repeated observations)');for(const month of dataset.months)option($('month'),month,monthName(month));$('month').value=dataset.months.at(-1);
    const names=new Map(dataset.rows.map(r=>[r.lga_code,r.lga_name]));for(const [code,name] of [...names].sort((a,b)=>a[1].localeCompare(b[1])))option($('lga'),code,name.replace(/\b\w/g,c=>c.toUpperCase()));
    for(const room of [...new Set(dataset.rows.map(r=>r.room_type))].sort())option($('room'),room,room);
    for(const id of ['month','lga','room','minimum'])$(id).disabled=false;
    const audit=dataset.audit,rejected=audit.input_rows-audit.accepted_rows;
    $('quality').innerHTML=`<strong>Data-quality reconciliation</strong><ul><li>${integer(audit.input_rows)} input rows → ${integer(audit.accepted_rows)} accepted listing-month records.</li><li>${integer(rejected)} rejected: ${integer(audit.rejected_source_month_mismatch||0)} source-month mismatches, ${integer(audit.rejected_invalid_price||0)} invalid prices, ${integer(audit.rejected_duplicate_listing_month||0)} duplicate keys.</li><li>${integer(audit.unmapped_rows||0)} accepted records missing an LGA mapping; ${integer(audit.missing_census_rows||0)} missing Census linkage.</li><li>${integer(audit.price_above_10000_rows||0)} asking prices above AUD 10,000 were retained and can influence means.</li></ul>`;
    $('fingerprint').textContent=`Aggregate data SHA-256: ${dataset.dataset_sha256}`;
    render();
  }catch(error){$('load-status').hidden=true;$('error').hidden=false;$('error').textContent=error.message;}
}
init();
