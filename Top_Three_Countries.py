from mrjob.job import MRJob
from mrjob.step import MRStep

class TopCountriesByGoldMedals(MRJob):

    def mapper(self, _, line):
        # Parse each line and extract relevant fields as semicolon is the delimiter
        parts = line.strip().split(';')
        if len(parts) >= 5:
            year = int(parts[2].strip())
            if 1980 <= year <= 2020:  # Filter records by year range
                country = parts[1].strip()
                medal = parts[4].strip()
                # Emit country and medal type
                yield country, medal

    def reducer_count_medals(self, country, medals):
        # Initialize medal counts
        total_gold = total_silver = total_bronze = 0
        for medal in medals:
            if medal == 'Gold':
                total_gold += 1
            elif medal == 'Silver':
                total_silver += 1
            elif medal == 'Bronze':
                total_bronze += 1
        # Emit country and their total medal counts
        yield None, (total_gold, total_silver, total_bronze, country)

    def reducer_find_top_countries(self, _, country_medal_counts):
        # Sort countries by gold medal count in descending order and pick top three
        sorted_countries = sorted(country_medal_counts, reverse=True, key=lambda x: x[0])[:3]
        # Emit the top three countries with their medal counts
        for gold, silver, bronze, country in sorted_countries:
            yield country, {"Gold": gold, "Silver": silver, "Bronze": bronze}

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_medals),
            MRStep(reducer=self.reducer_find_top_countries)
        ]

if __name__ == '__main__':
    TopCountriesByGoldMedals.run()
