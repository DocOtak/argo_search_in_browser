import { useState, useEffect } from "react";

import { parquetRead } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import KDBush from 'kdbush';
// @ts-ignore
import * as geokdbush from 'geokdbush-tk';  

function App() {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<any[][]>([]);
  const [index, setIndex] = useState<any>()
  const [position, setPosigin] = useState("0,0")

  useEffect(() => {
    fetch("/argo_index.parquet")
      .then((res) => res.arrayBuffer())
      .then((buffer) =>
        parquetRead({
          file: buffer,
          compressors,
          onComplete: (data) => {
            const idx = new KDBush(data.length)
            for (const [_fname, _date, latitude, longitude] of data){
              idx.add(longitude, latitude)
            }
            idx.finish()
            setIndex(idx)
            setData(data);
            setLoading(false);
          },
        })
      );
  }, []);
  let results = []
  if (!loading){
    try {
      const [lon, lat] = position.split(',')
      const lat_f = parseFloat(lat);
      const lon_f = parseFloat(lon)
      if ((lon_f != lon_f) || (lat_f != lat_f)){
        results = []
      } else {
      results = geokdbush.around(index, lon_f, lat_f, Infinity, 50)
      }
    } catch {
      results = []
    }
  }
  console.log(results)

  return (
    <>
      {loading ? "Loading... it's big (40MB)" : <span>Loaded {data.length} Profiles</span>}
      <br />
      Find profiles within 50 km of <input value={position} onChange={e => setPosigin(e.target.value)}/> (lon,lat)
      <br />
      {results.length} Profiles found
      <br />
      <table>
        <thead>
          <tr><th>File</th><th>Date</th><th>Latitude</th><th>Longitude</th></tr>
        </thead>
        <tbody>
        {results.map((idx:number) => {
            const row = data[idx]
            return <tr key={idx.toString()}>
              <td>{row[0]}</td>
              <td>{row[1].toISOString()}</td>
              <td>{row[2]}</td>
              <td>{row[3]}</td>
            </tr>
          })}
        </tbody>
      </table>
    </>
  );
}

export default App;
