import { next as A } from "@automerge/automerge";
import { writeFileSync } from "fs";

const gen1 = () => {
  const doc1 = A.from({ a: 1 }, { actor: "414141" });
  A.change(doc1, (doc) => (doc.a = 2));

  const changes = A.getAllChanges(doc1);

  // save changes to file
  const file = "change1.bin";
  changes.forEach((change) => {
    writeFileSync(file, change);
  });
};

const gen2 = () => {
  let doc1 = A.from({ a: 1, b: {} }, { actor: "414141" });
  doc1 = A.change(doc1, { time: 1741534262 }, (doc) => (doc.a = 2));
  doc1 = A.change(doc1, { time: 1741534262 }, (doc) => (doc.b = { a: 3 }));

  const changes = A.getAllChanges(doc1);

  // save changes to file
  const file = "change2.bin";
  changes.forEach((change) => {
    writeFileSync(file, change);
  });
};

const main = () => {
  gen1();
  gen2();
};

main();
