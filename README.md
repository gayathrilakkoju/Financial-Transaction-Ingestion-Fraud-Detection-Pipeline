 <!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Fraud Detection Pipeline</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      line-height: 1.6;
      margin: 40px;
      color: #222;
    }

    h1, h2, h3 {
      color: #111;
    }

    h1 {
      border-bottom: 2px solid #ddd;
      padding-bottom: 10px;
    }

    section {
      margin-bottom: 40px;
    }

    ul {
      margin-left: 20px;
    }

    .box {
      padding: 10px;
      background: #f7f7f7;
      border-left: 4px solid #007acc;
      margin: 10px 0;
    }

    .highlight {
      background: #fff3cd;
      padding: 10px;
      border-left: 4px solid #ffa500;
    }

    .note {
      color: #555;
      font-style: italic;
    }
  </style>
</head>

<body>

<h1>Financial Transaction Ingestion & Fraud Detection Pipeline</h1>

<section>
<h2>Project Background</h2>

<p>
This project simulates a financial system that monitors transaction activity and detects potential fraud.
In real banking and fintech systems, millions of transactions are processed daily. The main challenge is not just handling data, but identifying suspicious patterns while maintaining user experience.
</p>

<p>
In this project, I built a simplified version of that system — starting from raw transaction data, preprocessing it, and applying rule-based fraud detection to generate insights.
</p>

<div class="box">
<b>Focus Areas:</b>
<ul>
<li>Fraud Risk Distribution</li>
<li>High-Value Transactions</li>
<li>Transaction Behavior</li>
<li>System Effectiveness</li>
</ul>
</div>
</section>

<hr>

<section>
<h2>Data Structure & Initial Checks</h2>

<p>The dataset contains around <b>10,000 transaction records</b>.</p>

<h3>Fields:</h3>
<ul>
<li>transaction_id</li>
<li>transaction_date</li>
<li>amount</li>
<li>account_id</li>
<li>category</li>
<li>currency</li>
</ul>

<p>Before analysis, I ensured data consistency:</p>
<ul>
<li>Schema validation across all fields</li>
<li>Correct data types</li>
<li>Standardized categorical values</li>
</ul>

<p class="note">📌 Add dataset preview screenshot here</p>
</section>

<hr>

<section>
<h2>Executive Summary</h2>

<div class="highlight">
<p>
After processing ~10,000 transactions, the system identified a fraud rate of <b>5.21%</b>, which falls within a realistic financial range.
</p>

<p>
Fraud is not evenly distributed — it is mainly driven by high-value transactions and rapid transaction behavior patterns.
</p>
</div>

<p class="note">📊 Add final output screenshot here</p>
</section>

<hr>

<section>
<h2>Insights Deep Dive</h2>

<h3>Fraud Risk Distribution</h3>
<ul>
<li><b>Fraud Rate (~5.21%)</b> — realistic and well-calibrated</li>
<li>Balanced detection reduces false positives</li>
</ul>

<h3>High-Value Transactions</h3>
<ul>
<li>~962 transactions (~9.6%) exceeded ₹90,000</li>
<li>High financial impact despite lower volume</li>
<li>Often require manual review in real systems</li>
</ul>

<h3>Transaction Behavior</h3>
<ul>
<li>High-frequency transactions show higher risk</li>
<li>Behavioral patterns strongly indicate fraud risk</li>
<li>Amount + frequency = key signals</li>
</ul>

<h3>System Effectiveness</h3>
<ul>
<li>Rule-based system captures meaningful fraud signals</li>
<li>Scalable and adjustable pipeline design</li>
<li>Produces actionable insights, not just raw outputs</li>
</ul>

<p class="note">📊 Add charts/screenshots here</p>
</section>

<hr>

<section>
<h2>Recommendations</h2>

<ul>
<li>High-value transactions should have additional verification</li>
<li>Monitor transaction frequency along with amount</li>
<li>Regularly recalibrate fraud detection thresholds</li>
<li>Integrate machine learning for better accuracy</li>
<li>Use hybrid rule-based + ML systems for best performance</li>
</ul>
</section>

<hr>

<section>
<h2>Assumptions & Caveats</h2>

<ul>
<li>Synthetic dataset used (realistic behavior simulated)</li>
<li>Rule-based detection may include false positives</li>
<li>Thresholds tuned for ~1–5% fraud range</li>
<li>No internal ledger reconciliation included</li>
<li>Transaction timestamps assumed accurate</li>
</ul>
</section>

</body>
</html>
