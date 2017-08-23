#!/usr/bin/perl

my $dir='.';

my @files = glob("*.pdf");

foreach my $file (@files) {
  $file=~m/(\d*)\.pdf/;
  my $brokerid=$1;

  next if (!$brokerid);

  open(PROSPECT, "pdftotext -nopgbrk $file -eol mac - | tr  ' ' |");

  my $name="";
  my $cnpj="";
  my $tx="";
  while (<PROSPECT>) {
      /^(.*?) CNPJ.*?(\d.*?) /;
      $name=$1;
      $cnpj=$2;

      $cnpj=~s/[\.\/\-]//g;
      $cnpj="" if ($cnpj=~m/\,/);

      if (!$tx) {
         /[Tt]axa de [aA]dministração:.*? (\d.*?)%/;
         $tx=$1;
      }

      if (!$tx) {
         /jus .*? [Tt]axa de [aA]dministração.*? (\d.*?)%/;
         $tx=$1;
      }

      if ($tx) {
         $tx=~s/\,/./g;
         $tx/=100;
      }
  }

  close(PROSPECT);

  print "$brokerid|$cnpj|$tx|$name\n";

}

exit 0;
