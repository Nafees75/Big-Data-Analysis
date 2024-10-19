from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTopMedalists(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_aggregate_medals),
            MRStep(reducer=self.reducer_find_top_three)
        ]

    def mapper(self, _, line):
        # Each line has the format: athlete_id; country; year; event; medal
        data = line.split(';')
        athlete_id = data[0].strip()
        medal = data[4].strip()
        # Emit tuple (athlete_id, medal) as key and 1 as count
        yield (athlete_id, medal), 1

    def reducer_aggregate_medals(self, athlete_medal, counts):
        # Sum up all the counts for each athlete and medal type
        yield athlete_medal[1], (sum(counts), athlete_medal[0])

    def reducer_find_top_three(self, medal, count_athlete_pairs):
        # Sort athletes by medal count in descending order and pick the top three
        top_three = sorted(count_athlete_pairs, reverse=True, key=lambda x: x[0])[:3]
        for count, athlete in top_three:
            yield medal, (athlete, count)

if __name__ == '__main__':
    MRTopMedalists.run()
