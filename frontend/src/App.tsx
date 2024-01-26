import "./App.css";
import { Container, Row, Col, Button } from "react-bootstrap";
import React, { useState, useEffect } from "react";
import ChartCustom from "./components/first_chart";

function App() {
  const [selectedChart, setSelectedChart] = useState(1);
  const [selectedAmount, setSelectedAmount] = useState(10);
  const [chartData, setChartData] = useState({});

  const sortChartData = (chartData: any) => {
    const sortedChartData = Object.entries(chartData).sort(
      (a: any, b: any) => b[1] - a[1]
    );
    setChartData(sortedChartData);
  };

  // Fetch data from Flask backend and set it to chartData
  useEffect(() => {
    const fetchDataFromFlask = async () => {
      // Example: Fetch data from Flask API with dynamic selectedChart value
      const response = await fetch(
        `http://192.168.50.79:5000/get_transformed_data/${selectedChart}/${selectedAmount}`
      );
      const data = await response.json();
      setChartData(data.data);
    };
    fetchDataFromFlask();

    sortChartData(chartData);
  }, [selectedChart, selectedAmount]);

  return (
    <div className="App">
      <Container>
        <Row
          style={{
            justifyContent: "space-between",
            alignItems: "center",
            margin: "10px 0",
          }}
        >
          <Col>
            <h1>Raimonds Silins final project</h1>
          </Col>
        </Row>
        <Row>
          <Row>
            <h1>Example data visualisation</h1>
          </Row>
          {chartData ? (
            <Row>
              <h1>
                <h1>
                  {(() => {
                    // Switch between 3 charts
                    switch (selectedChart) {
                      case 1:
                        return "Percentage of lost population from Covid-19";
                      case 2:
                        return "Amount of fully vaccinated";
                      case 3:
                        return "Development ratio and mortality rate";
                      default:
                        return "Default Title";
                    }
                  })()}
                </h1>
              </h1>
              <ChartCustom
                chartLabel={(() => {
                  // Switch between 3 charts
                  switch (selectedChart) {
                    case 1:
                      return "Percentage of lost population from Covid-19";
                    case 2:
                      return "Amount of fully vaccinated";
                    case 3:
                      return "Development ratio and mortality rate";
                    default:
                      return "Default Title";
                  }
                })()}
                data={chartData}
              />
              <Row>
                <h3>
                  Right now it is set on top {selectedAmount} countries. Set the
                  needed amount below.
                </h3>
                <input
                  type="number"
                  min="1"
                  max="100"
                  value={selectedAmount}
                  onChange={(e) => {
                    if (parseInt(e.target.value) > 0)
                      setSelectedAmount(parseInt(e.target.value));
                  }}
                />
              </Row>
              <Row
                style={{ justifyContent: "space-between", marginTop: "10px" }}
              >
                <Button
                  disabled={selectedChart === 1}
                  onClick={() => {
                    if (selectedChart > 1) setSelectedChart(selectedChart - 1);
                  }}
                >
                  PREVIOUS CHART
                </Button>
                <Button
                  onClick={() => {
                    if (selectedChart < 3) setSelectedChart(selectedChart + 1);
                  }}
                >
                  NEXT CHART
                </Button>
              </Row>
            </Row>
          ) : (
            "Loading..."
          )}
        </Row>
      </Container>
    </div>
  );
}

export default App;
