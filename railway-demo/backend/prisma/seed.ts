import { PrismaClient } from "@prisma/client";

// Creates the two demo trains. Re-running this never changes existing seat counts,
// so it is safe to run after bookings have been made.
const prisma = new PrismaClient();

const trains = [
  { trainId: 1, trainName: "Express 101", availableSeats: 1 },
  { trainId: 2, trainName: "Local 202", availableSeats: 5 },
];

for (const train of trains) {
  await prisma.train.upsert({
    where: { trainId: train.trainId },
    update: {},
    create: train,
  });
}

await prisma.$disconnect();
console.log("Seeded trains: Express 101 (1 seat), Local 202 (5 seats).");
