// src/index.ts
import app from './server';

const port: number = 3000;

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});