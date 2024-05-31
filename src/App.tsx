import { useState, useEffect } from "react";

import { parquetRead } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import KDBush from "kdbush";
// @ts-ignore
import * as geokdbush from "geokdbush-tk";

const chunkPaths = [
  "argo_index.parquet.aa",
  "argo_index.parquet.ab",
  "argo_index.parquet.ac",
  "argo_index.parquet.ad",
  "argo_index.parquet.ae",
  "argo_index.parquet.af",
  "argo_index.parquet.ag",
  "argo_index.parquet.ah",
  "argo_index.parquet.ai",
  "argo_index.parquet.aj",
  "argo_index.parquet.ak",
  "argo_index.parquet.al",
  "argo_index.parquet.am",
  "argo_index.parquet.an",
  "argo_index.parquet.ao",
  "argo_index.parquet.ap",
  "argo_index.parquet.aq",
  "argo_index.parquet.ar",
  "argo_index.parquet.as",
  "argo_index.parquet.at",
  "argo_index.parquet.au",
  "argo_index.parquet.av",
  "argo_index.parquet.aw",
  "argo_index.parquet.ax",
  "argo_index.parquet.ay",
  "argo_index.parquet.az",
  "argo_index.parquet.ba",
  "argo_index.parquet.bb",
  "argo_index.parquet.bc",
  "argo_index.parquet.bd",
  "argo_index.parquet.be",
  "argo_index.parquet.bf",
  "argo_index.parquet.bg",
  "argo_index.parquet.bh",
  "argo_index.parquet.bi",
];

function App() {
  const [loading, setLoading] = useState(true);
  const [chunks, setChunks] = useState<Blob[]>([]);
  const [data, setData] = useState<any[][]>([]);
  const [index, setIndex] = useState<any>();
  const [position, setPosigin] = useState("0,0");

  if (chunks.length !== chunkPaths.length) {
    fetch(chunkPaths[chunks.length])
      .then((res) => res.blob())
      .then((blob) => setChunks([...chunks, blob]));
  } else {
    new Blob(chunks).arrayBuffer().then((buffer) =>
      parquetRead({
        file: buffer,
        compressors,
        onComplete: (data) => {
          const idx = new KDBush(data.length);
          for (const [_fname, _date, latitude, longitude] of data) {
            idx.add(longitude, latitude);
          }
          idx.finish();
          setIndex(idx);
          setData(data);
          setLoading(false);
        },
      })
    );
  }
  let results = [];
  if (!loading) {
    try {
      const [lon, lat] = position.split(",");
      const lat_f = parseFloat(lat);
      const lon_f = parseFloat(lon);
      if (lon_f != lon_f || lat_f != lat_f) {
        results = [];
      } else {
        results = geokdbush.around(index, lon_f, lat_f, Infinity, 50);
      }
    } catch {
      results = [];
    }
  }
  console.log(results);

  return (
    <>
      {loading ? (
        <span>
          Loading chunk {chunks.length} of {chunkPaths.length} { chunks.length === chunkPaths.length && "Indexing..."}
        </span>
      ) : (
        <span>Loaded {data.length} Profiles</span>
      )}
      <br />
      Find profiles within 50 km of{" "}
      <input
        value={position}
        onChange={(e) => setPosigin(e.target.value)}
      />{" "}
      (lon,lat)
      <br />
      {results.length} Profiles found
      <br />
      <table>
        <thead>
          <tr>
            <th>File</th>
            <th>Date</th>
            <th>Latitude</th>
            <th>Longitude</th>
          </tr>
        </thead>
        <tbody>
          {results.map((idx: number) => {
            const row = data[idx];
            return (
              <tr key={idx.toString()}>
                <td>{row[0]}</td>
                <td>{row[1].toISOString()}</td>
                <td>{row[2]}</td>
                <td>{row[3]}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </>
  );
}

export default App;
